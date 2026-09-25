from __future__ import annotations
from datetime import datetime
import pandas as pd
from erp_pipeline.model.load_mode import LoadMode
from erp_pipeline.load_data.data_loaders import load_latest
from erp_pipeline.process.dataset_specific import resolve_part_extrafields_by_group_name,fill_value_from_parent_row
from erp_pipeline.save import save_combined_parquet
from erp_pipeline.file_cleaner import clean_parquet_history
from pathlib import Path
from typing import Optional

COMBINED_PATH = Path("/opt/airflow/results/combined")

def combine_manufacturing_data(staged_manufacturing_path: Path, staged_common_path: Path, staged_sales_path: Path, mapping: dict, combined_path: Optional[Path] = None):

    combined_path = combined_path or COMBINED_PATH

    _, manufacturing_orders = load_latest(
        staged_manufacturing_path, "ManufacturingOrders", LoadMode.STAGED, mapping["ManufacturingOrders"]
    )
    _, orderOperations = load_latest(
        staged_manufacturing_path, "ManufacturingOrderOperations", LoadMode.STAGED, mapping["ManufacturingOrderOperations"]
    )
    _, operationRows = load_latest(
        staged_manufacturing_path, "OperationRows", LoadMode.STAGED, mapping["OperationRows"]
    )

    _, workCenters = load_latest(
        staged_manufacturing_path, "WorkCenters", LoadMode.STAGED, mapping["WorkCenters"]
    )

    _, extraFields = load_latest(
        staged_common_path, "ExtraFields", LoadMode.STAGED, mapping["ExtraFields"]
    )

    _, extraFieldGroups = load_latest(
        staged_common_path, "ExtraFieldGroups", LoadMode.STAGED, mapping["ExtraFieldGroups"]
    )

    _, extraFieldOptionTemplates = load_latest(
        staged_common_path, "ExtraFieldOptionTemplates", LoadMode.STAGED, mapping["ExtraFieldOptionTemplates"]
    )

    _, extraFieldTemplates = load_latest(
        staged_common_path, "ExtraFieldTemplates", LoadMode.STAGED, mapping["ExtraFieldTemplates"]
    )
    _, rows = load_latest(
        staged_sales_path, "CustomerOrderRows",LoadMode.STAGED, mapping["CustomerOrderRows"]
    )

    manufacturing_orders = manufacturing_orders.rename(columns={
        "Id": "ManufacturingOrderId"
    })

    orderOperations = orderOperations.rename(columns={
        "Id": "ManufacturingOrderOperationId",
        "ReportedQuantity": "OperationReportedQuantity",
        "PlannedQuantity": "OperationPlannedQuantity",
        "PartId": "OperationPartId"
    })

    operationRows = operationRows.rename(columns={
        "Id": "OperationRowId"
    })

    
    workCenters = workCenters.rename(columns={
        "Id": "WorkCenterId",
        "Number": "WorkCenterNumber"
    })

    rows = rows.rename(columns={
        "Id": "CustomerOrderRowId"
    })

    rows = (
        rows[["CustomerOrderRowId", "LinkedManufacturingOrderId"]]
        .drop_duplicates(subset=["LinkedManufacturingOrderId"])
    )

    manufacturing_orders = manufacturing_orders.drop_duplicates(subset=["ManufacturingOrderId"])

    manufacturing_orders = manufacturing_orders.merge(
        rows[["CustomerOrderRowId", "LinkedManufacturingOrderId"]],
        left_on="ManufacturingOrderId",
        right_on="LinkedManufacturingOrderId",
        how="left"
    )


    part_extrafields = resolve_part_extrafields_by_group_name(
        extraFieldGroups=extraFieldGroups,
        extraFieldTemplates=extraFieldTemplates,
        extraFieldOptionTemplates=extraFieldOptionTemplates,
        extraFields=extraFields,
        group_name="lisätiedot"
    )

    manufacturing_orders = manufacturing_orders.merge(
    part_extrafields,
    on="PartId",
    how="left"
    )

    merged = (
        orderOperations
        .merge(
            manufacturing_orders,
            on="ManufacturingOrderId",
            how="left"
        )
        .merge(
            operationRows,
            on="OperationRowId",
            how="left"
        )
        .merge(
            workCenters,
            on="WorkCenterId",
            how="left"

        )
    )

    merged["ActualFinishDate"] = pd.to_datetime(
        merged["ActualFinishDate"],
        errors="coerce",
        utc=True
    )

    merged["FinishYear"] = merged["ActualFinishDate"].dt.year

    time_cols = [
        "ReportedUnitTime",
        "ReportedSetupTime",
        "PlannedUnitTime",
        "PlannedSetupTime",
    ]

    for col in time_cols:
        if col in merged.columns:
            merged[col] = (
                merged[col]
                .astype("string")
                .str.strip()
                .str.replace(",", ".", regex=False)
            )
            merged[col] = pd.to_numeric(merged[col], errors="coerce")


    date_str = datetime.now().strftime("%Y-%m-%d")

    merged["ActualFinishDate"] = merged["ActualFinishDate"].dt.tz_localize(None)

    stamping_agg = (
        merged
        .groupby("ManufacturingOrderOperationId", as_index=False)
        .agg(
            PlannedUnitTimeSum=("PlannedUnitTime", "sum"),
            PlannedSetupTimeSum=("PlannedSetupTime", "sum"),
            ReportedUnitTimeSum=("ReportedUnitTime", "sum"),
            ReportedSetupTimeSum=("ReportedSetupTime", "sum")
        )
    )

    
    merged = merged.merge(
        stamping_agg,
        on="ManufacturingOrderOperationId",
        how="left"
    )

    endpoint = "manufacturing"
    saved_parquet = save_combined_parquet(
        merged,
        date_str,
        combined_path/ endpoint,
        endpoint
    )

    clean_parquet_history(endpoint, combined_path / endpoint, days_to_keep=2)


    return saved_parquet

def combine_sales_data(staged_path: Path,manufacturing_path : Path, mapping: dict, combined_path: Optional[Path] = None):

    combined_path = combined_path or COMBINED_PATH
    date_rows, rows = load_latest(staged_path, "CustomerOrderRows",LoadMode.STAGED, mapping["CustomerOrderRows"])
    date_orders, orders = load_latest(staged_path, "CustomerOrders",LoadMode.STAGED, mapping["CustomerOrders"])
    date_customers, customers = load_latest(staged_path, "Customers",LoadMode.STAGED, mapping["Customers"])
    _, invoices = load_latest(staged_path, "InvoiceLogs",LoadMode.STAGED, mapping["InvoiceLogs"])
    _, manufacturing = load_latest(manufacturing_path, "ManufacturingOrders",LoadMode.STAGED,mapping["ManufacturingOrders"])
    _, manufacturing_operations = load_latest(manufacturing_path, "ManufacturingOrderOperations",LoadMode.STAGED,mapping["ManufacturingOrderOperations"])

    if len({date_rows, date_orders, date_customers}) > 1:
        print("⚠ WARNING: Source dates differ! %s %s %s", date_rows, date_orders, date_customers)


    rows = rows.rename(columns={
        "Id": "OrderRowId",
        "OrderDate": "OrderRowDate",
        "Status" : "CustomerOrderRowStatus"
    })

    rows = fill_value_from_parent_row(rows)
    rows = fill_value_from_parent_row(rows, value_col="LinkedManufacturingOrderId")
    rows = rows[rows["OrderRowType"] != 4]
    orders = orders.rename(columns={
        "Id": "OrderId",
        "Status" : "CustomerOrderStatus"
    })

    customers = customers.rename(columns={
        "Id": "CustomerRecordId"
    })

    invoices = invoices.rename(columns={
        "Id" : "InvoiceId",
        "CustomerOrderId" : "InvoiceOrderId",
        "Price" : "InvoicePrice",
        "LoggingTimestamp" : "InvoiceTimestamp"
    })

    manufacturing = manufacturing.rename(columns={
        "Id" : "ManufacturingOrderId",
        "CustomerOrderId" : "ManufacturingCustomerOrderId",
        "OrderNumber" : "ManufacturingOrderNumber",
        "Status" : "ManufacturingOrderStatus"
    })

    manufacturing_operations = manufacturing_operations.rename(columns={
        "Id" : "OperationId",
        "ManufacturingOrderId" : "OperationManufacturingOrderId"
    })

    
    invoice_agg = (
        invoices
        .groupby("InvoiceOrderId", as_index=False)
        .agg({
            "InvoiceDate" : "first",
            "InvoicePrice": "sum",
            "InvoicedQuantity": "sum",
            "InvoiceTimestamp" : "max"
        })
    )
    
    manufacturing_agg = (
        manufacturing
        .groupby("ManufacturingCustomerOrderId", as_index=False)
        .agg({
            "ManufacturingOrderNumber" : "first",
            "ManufacturingOrderStatus" : "first",
            "StartDate": "max",
            "FinishDate": "max",
            "InitialFinishDate": "max"
        })
    )
    
    manufacturing_operations["ActualStartDate"] = pd.to_datetime(
        manufacturing_operations["ActualStartDate"],
        errors="coerce",
        utc=True
    ).dt.tz_convert("Europe/Helsinki")

    manufacturing_operations["ActualFinishDate"] = pd.to_datetime(
        manufacturing_operations["ActualFinishDate"],
        errors="coerce",
        utc=True
    ).dt.tz_convert("Europe/Helsinki")

    manufacturing_operation_agg = (
        manufacturing_operations
        .groupby("OperationManufacturingOrderId", as_index=False)
        .agg({
            "ActualStartDate": "max",
            "ActualFinishDate": "max"
        })
    )

    manufacturing_with_ops = manufacturing.merge(
        manufacturing_operation_agg,
        left_on="ManufacturingOrderId",
        right_on="OperationManufacturingOrderId",
        how="left"
    )

    manufacturing_full_agg = (
        manufacturing_with_ops
        .groupby("ManufacturingCustomerOrderId", as_index=False)
        .agg({
            "ActualStartDate": "max",
            "ActualFinishDate": "max"
        })
    )

    merged = rows.merge(
        orders,
        left_on="ParentOrderId",
        right_on="OrderId"
    )

    merged = merged.merge(
        manufacturing_agg,
        left_on="OrderId",
        right_on="ManufacturingCustomerOrderId",
        how="left"
    )

    merged = merged.merge(
        manufacturing_full_agg,
        left_on="OrderId",
        right_on="ManufacturingCustomerOrderId",
        how="left"
    )

    merged = merged.merge(
        invoice_agg,
        left_on="OrderId",
        right_on="InvoiceOrderId",
        how="left"
    )


    merged = merged.merge(
        customers,
        left_on="CustomerId",
        right_on="CustomerRecordId"
    )
    
    for col in merged.select_dtypes(include=["datetimetz"]).columns:
        merged[col] = merged[col].dt.tz_localize(None)

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "sales"
    saved_parquet =  save_combined_parquet(merged,date_str,combined_path / endpoint,endpoint)
    clean_parquet_history(endpoint, combined_path / endpoint, days_to_keep=2)
    return saved_parquet

def combine_inventory_data(staged_path: Path, mapping: dict,combined_path: Optional[Path] = None):

    combined_path = combined_path or COMBINED_PATH

    date_codes, part_codes = load_latest(staged_path, "PartCodes",LoadMode.STAGED, mapping["PartCodes"])
    date_parts, parts = load_latest(staged_path, "Parts",LoadMode.STAGED, mapping["Parts"])

    if len({date_codes, date_parts}) > 1:
        print("⚠ WARNING: Source dates differ! %s %s", date_codes, date_parts)

    merged = parts.merge(
        part_codes,
        left_on="PartCodeId",
        right_on="Id",
        how="left",
        suffixes=("_part", "_code")
    )

    merged = merged.drop(columns=["Id_code"], errors="ignore")

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "inventory"

    saved_parquet =  save_combined_parquet(merged,date_str,combined_path / endpoint,endpoint)
    clean_parquet_history(endpoint, combined_path / endpoint, days_to_keep=2)
    return saved_parquet

def combine_work_interval_data(
        staged_work_interval_path: Path,
        staged_manufacturing_path: Path,
        mapping: dict,combined_path: Optional[Path] = None):

    combined_path = combined_path or COMBINED_PATH

    _, manufacturing_orders = load_latest(
            staged_manufacturing_path,
            "ManufacturingOrders",
            LoadMode.STAGED,
            mapping["ManufacturingOrders"]
        )
    
    _, order_operations = load_latest(
            staged_manufacturing_path,
            "ManufacturingOrderOperations",
            LoadMode.STAGED,
            mapping["ManufacturingOrderOperations"]
        )
    
    _, work_intervals = load_latest(
            staged_work_interval_path,
            "WorkIntervals",
            LoadMode.STAGED,
            mapping["WorkIntervals"]
        )

    order_operations = order_operations.rename(columns={
        "Id": "OrderOperationId",
        "PartId" : "OrderOperationPartId",
        
        })
    manufacturing_orders = manufacturing_orders.rename(columns={
        "Id": "Id_manufacturing_order",
        "PartId" : "ManufacturingOrderPartId"
            
        })

    manufacturing_orders = manufacturing_orders.drop_duplicates(subset=["Id_manufacturing_order"])
    
    work_fact = (
        work_intervals
        .merge(
            order_operations,
            left_on="OperationId",
            right_on="OrderOperationId",
            how="left"
        )
        .merge(
            manufacturing_orders,
            left_on="ManufacturingOrderId",
            right_on="Id_manufacturing_order",
            how="left",
            suffixes=("_op", "_order")
        )
    )
    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "time_recording"
    saved_parquet = save_combined_parquet(
        work_fact,
        date_str,
        combined_path / endpoint,
        endpoint
    )
    
    clean_parquet_history(endpoint, combined_path / endpoint, days_to_keep=2)
    
    
    return saved_parquet

def combine_material_data(combined_sales_path: Path,staged_path: Path, mapping: dict,combined_path: Optional[Path] = None):

    combined_path = combined_path or COMBINED_PATH
    
    combined_sales = pd.read_parquet(combined_sales_path)
    date_materials, materials = load_latest(staged_path, "StockTransactions",LoadMode.STAGED, mapping["StockTransactions"])

    if len({date_materials}) > 1:
        print("⚠ WARNING: Source dates differ!", date_materials)


    merged = materials.merge(
        combined_sales,
        left_on="ParentOrderId",
        right_on="OrderId"
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "sales"
    saved_parquet =  save_combined_parquet(merged,date_str,combined_path / endpoint,endpoint)
    clean_parquet_history(endpoint, combined_path / endpoint, days_to_keep=2)
    return saved_parquet


def combine_purchase_data(staged_path: Path, mapping: dict,combined_path: Optional[Path] = None):

    combined_path = combined_path or COMBINED_PATH

    _, purchase_orders = load_latest(
        staged_path,
        "PurchaseOrders",
        LoadMode.STAGED,
        mapping["PurchaseOrders"]
    )

    _, purchase_order_rows = load_latest(
        staged_path,
        "PurchaseOrderRows",
        LoadMode.STAGED,
        mapping["PurchaseOrderRows"]
    )


    purchase_orders = purchase_orders.rename(columns={
        "Id": "PurchaseOrderId",
        "OrderDate" : "PurchaseOrderDate",
        "WarehouseId" : "PurchaseOrderWarehouseId",
        "IsConfirmed" : "PurchaseOrderIsConfirmed"
    })

    purchase_order_rows = purchase_order_rows.rename(columns={
        "Id": "PurchaseOrderRowId",
        "OrderDate" : "PurchaseOrderRowDate",
        "WarehouseId" : "PurchaseOrderRowWarehouseId",
        "IsConfirmed" : "PurchaseOrderRowIsConfirmed"
    })

    
    merged = purchase_order_rows.merge(
        purchase_orders,
        left_on="ParentOrderId",
        right_on="PurchaseOrderId",
        how="left"
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    endpoint = "purchase"

    saved = save_combined_parquet(
        merged,
        date_str,
        combined_path / endpoint,
        endpoint
    )

    clean_parquet_history(endpoint, combined_path / endpoint, days_to_keep=7)

    return saved
ERP_COMMON_SOURCE_TYPES = [
    "erp_common_currencies",
    "erp_common_extrafieldgroups",
    "erp_common_extrafieldoptiontemplates",
    "erp_common_extrafields",
    "erp_common_extrafieldtemplates",
    "erp_common_reasoncodes",
    "erp_common_units",
    "erp_common_departments"
]

ERP_INVENTORY_SOURCE_TYPES = [
    "erp_inventory_partcodes",
    "erp_inventory_parts",
]

ERP_MANUFACTURING_SOURCE_TYPES = [
    "erp_manufacturing_drawings",
    "erp_manufacturing_manualOperationStatuses",
    "erp_manufacturing_orderMaterials",
    "erp_manufacturing_orderNodeDrawings",
    "erp_manufacturing_orderNodes",
    "erp_manufacturing_orderOperationControlDataRows",
    "erp_manufacturing_orderOperationReportings",
    "erp_manufacturing_orderOperations",
    "erp_manufacturing_orders",
    "erp_manufacturing_orderTypes",
    "erp_manufacturing_materialRows",
    "erp_manufacturing_operationRowControlDataRows",
    "erp_manufacturing_operationRows",
     "erp_manufacturing_orderQuantityManufacturingOrderOperations",
    "erp_manufacturing_orderQuantityManufacturingOrders",
    "erp_manufacturing_preparations",
    "erp_manufacturing_productionCalendarExceptions",
    "erp_manufacturing_reasonCodeTimeConsumptions",
    "erp_manufacturing_workCenterCostFactorGroups",
    "erp_manufacturing_workCenters",
]

ERP_SALES_SOURCE_TYPES= [
    "erp_sales_customerOrderRows",
    "erp_sales_customerOrders",
    "erp_sales_customerOrderTypes",
    "erp_sales_customers",
    "erp_sales_invoiceLogs",
    "erp_sales_orderQuantityCustomerOrders",
    "erp_sales_salesPrices",
]



ERP_SOURCE_CONFIG = {
    "common":{
        "Currencies" : ["Id","Code","Description"],
        "ExtraFieldGroups" : ["Id","Name","RowNumber"],
        "ExtraFieldOptionTemplates" : ["Id","Code","Description","ExtraFieldTemplateId"],
        "ExtraFields" : ["Id","ParentId","SelectedOptionId","SelectedOptionIds"],
        "ExtraFieldTemplates" : ["Id","Name","ParentId","RowNumber"],
        "ReasonCodes" : ["Id","Description","Code"],
        "Units" : ["Id","Code","Description","Number"],
        "Departments" : ["Id","Code","Description"],
    },

    "inventory": {
        "PartCodes" : ["Id","Alias","Code","Description"],
        "Parts" : ["Id","PartNumber","Description","ActiveRevisionId","Alias","AveragePrice","CompanyId","PartCodeId","PreparationId","StandardPrice","ExtraDescription"],
    },

    "manufacturing" : {
        "Drawings" : ["Id","DrawingNumber","PartId"],
        "ManualOperationStatuses" : ["Id","Code","Description"],
        "ManufacturingOrderMaterials" : ["Id","PartId"],
        "ManufacturingOrderNodeDrawings" : [],
        "ManufacturingOrderNodes" : ["Id","PartId","ReportedQuantity","PlannedQuantity","RestQuantity","StartDate","EndDate","WarehouseId","ManufacturingOrderNodeId"],
        "ManufacturingOrderOperationControlDataRows" : [],
        "ManufacturingOrderOperationReportings" : ["Id","OperationId","Quantity","SetupTime","UnitTime","WorkCenterId","WarehouseId","EmployeeId","ActualReportedDate",
                                                        "PreviousRestQuantity","RestQuantity","MainPartId","UnitCost","SetupCost","ReportingTimeStamp","CommentId"],
        "ManufacturingOrderOperations" : ["Id","Priority",#"WorkCenterId","PartId","PlannedQuantity","ReportedQuantity",
                                                "RestQuantity","ReportedUnitTime",
                                                "ReportedSetupTime","RejectedQuantity","ManufacturingOrderId","OperationNumber","PlannedSetupTime","PlannedUnitTime",
                                                "PlannedStartDate","PlannedFinishDate","ActualStartDate","ActualFinishDate","ManufacturingOrderNodeId","Status",
                                                "OperationRowId","Description"],
        "ManufacturingOrders" : ["Id","OrderNumber","Status","WarehouseId","PartId","ReportedQuantity","PlannedQuantity","RestQuantity","StartDate","FinishDate",
                                    "CustomerOrderId","CustomerId","InitialFinishDate","OrderTypeId"],
        "ManufacturingOrderTypes" : ["Id","Description"],
        "MaterialRows" : ["Id","PartId","Quantity","SetupQuantity"],
        "OperationRowControlDataRows" : [],
        "OperationRows" : ["Id","OperationNumber","Description","WorkCenterId","IsSubContractor","SetupTime","UnitTime","QueueTime","UnitCost"],
        "OrderQuantityManufacturingOrderOperations" : [],
        "OrderQuantityManufacturingOrders" : [],
        "Preparations" : ["Id","PartNumber","Description"],
        "ProductionCalendarExceptions" : ["Id","WorkCenterId","ExceptionDate","BasicTime"],
        "ReasonCodeTimeConsumptions" : [],
        "WorkCenterCostFactorGroups" : ["Id"],
        "WorkCenters" : ["Id","Number","Description","OperationDecription","BasicTime"],
    },
    
    "sales" : {
        "CustomerOrderRows" : ["Id","PartId","DeliveredQuantity","DeliveryDate","DesiredDeliveryDate","LinkedManufacturingOrderId","InitialDeliveryDate","OrderDate","OrderedQuantity","OrderRowType",
                                     "ParentOrderId","ParentRowId","Price","PriceInCompanyCurrency","RevisionId","NewFinishDate","UnitId"],
        "CustomerOrders" : ["Id","CustomerId","InvoiceCustomerId","OrderDate","OrderNumber","OrderTypeId","Status","SellerId"],
        "CustomerOrderTypes" : ["Id","Description","IncludeInStatistics"],
        "Customers" : ["Id","Name","Code","AlternativeName"],
        "InvoiceLogs" : ["Id","InvoiceId","InvoiceDate","PartId","PartNumber","PartCode","CustomerId","CustomerOrderId","OrderedQuantity","InvoicedQuantity","UnitCode","Price","PriceInCompanyCurrency","StandardPrice"],
        "OrderQuantityCustomerOrders" : [],
        "SalesPrices" : [],
    }
}

euro_columns = {
    "Parts" : ["AveragePrice", "StandardPrice"],
    "OrderOperationReportings" : ["SetupCost","UnitCost"],
    "ManufacturingOrderRows" : ["SetupCost","UnitCost"],
    "CustomerOrderRows" : ["Price","PriceInCompanyCurrency"],
    "InvoiceLogs" : ["Price","SetupPriceInCompanyCurrency","StandardPrice"],
   
}

combine_sales_tables = {
    "CustomerOrderRows" : ["Id","PartId","DeliveredQuantity","DeliveryDate","DesiredDeliveryDate","LinkedManufacturingOrderId","InitialDeliveryDate","OrderDate","OrderedQuantity","OrderRowType",
                                     "ParentOrderId","ParentRowId","Price","PriceInCompanyCurrency","RevisionId","NewFinishDate","UnitId"],
    "CustomerOrders" : ["Id","CustomerId","InvoiceCustomerId","OrderDate","OrderNumber","OrderTypeId","Status","SellerId"],
    "Customers" : ["Id","Name","Code","AlternativeName"],
}

combine_manufacturing_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","OrderTypeId","CustomerOrderId","CustomerId","InitialFinishDate","Status","PartId","ReportedQuantity","PlannedQuantity",],
    "ManufacturingOrderOperations" : ["Id","WorkCenterId","PartId",
    "PlannedQuantity","ReportedQuantity","RejectedQuantity","RestQuantity",
    "ReportedUnitTime","PlannedUnitTime","ReportedSetupTime","PlannedSetupTime",
    "ManufacturingOrderId","OperationNumber","OperationRowId","PlannedStartDate","ActualStartDate",
    "PlannedFinishDate","ActualFinishDate","WorkshopOperationStatus","Description"],
    
    "OperationRows" : ["Id","OrderOperation","UnitTime"]

}

combine_reporting_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","OrderTypeId","CustomerOrderId","CustomerId","InitialFinishDate","Status","PartId","ReportedQuantity","PlannedQuantity",],
    "ManufacturingOrderOperationReportings" : ["Id","OperationId","Quantity","SetupTime","UnitTime","WorkCenterId","WarehouseId","EmployeeId","ActualReportedDate",
                                                        "PreviousRestQuantity","RestQuantity","MainPartId","UnitCost","SetupCost","ReportingTimeStamp","CommentId"],
    "ManufacturingOrderOperations" : ["Id","WorkCenterId","PartId",
    "PlannedQuantity","ReportedQuantity","RejectedQuantity","RestQuantity",
    "ReportedUnitTime","PlannedUnitTime","ReportedSetupTime","PlannedSetupTime",
    "ManufacturingOrderId","OperationNumber","OperationRowId","PlannedStartDate","ActualStartDate",
    "PlannedFinishDate","ActualFinishDate","WorkshopOperationStatus","Description"],
}

combine_inventory_tables = {
    "PartCodes" : ["Id","Alias","Code","Description"],
    "Parts" : ["Id","PartNumber","Description","ActiveRevisionId","Alias","AveragePrice","CompanyId","PartCodeId","StandardPrice","ExtraDescription"]
}


#Datamodel

fact_order_rows = [
    "order_row_id",
    "order_number",
    "work_number",
    "customer_id",
    "seller_id",
    "article_id",
    "order_created_date",
    "start_date_planned",
    "start_date_actual",
    "delivery_date_planned",
    "delivery_date_actual",
    "order_state_id",
    "order_type_id",
    "ordered_qty",
    "total_price",
    "unit_price",
    "unit_price_cost_planned",
    "unit_prime_cost_planned_sum",
    "unit_prime_cost_actual",
    "prime_cost_actual_sum",
    "unit",
    "currency",
    "sales_margin_planned",
    "sales_margin_actual",
    "new_article",
    "order_complete"
]
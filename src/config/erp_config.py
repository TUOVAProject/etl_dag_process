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
    "erp_sales_customerOrderInvoiceRows",
    "erp_sales_salesPrices",
]

ERP_PURCHASE_SOURCE_TYPES=[
    "erp_purchase_orderRows",
    "erp_puchase_orders",
    "erp_purchase_orderInvoiceRows",
    "erp_purchase_orderInvoices",
    "erp_purchase_orderTypes"
]



ERP_SOURCE_CONFIG = {
    "common":{
        "Currencies" : ["Id","Code","Description"],
        "ExtraFieldGroups" : ["Id","Name","RowNumber"],
        "ExtraFieldOptionTemplates" : ["Id","Code","Description","ExtraFieldTemplateId"],
        "ExtraFields" : ["Id","ParentId","ParentClass","SelectedOptionId","SelectedOptionIds"],
        "ExtraFieldTemplates" : ["Id","Name","ParentId","RowNumber"],
        "ReasonCodes" : ["Id","Description","Code"],
        "Units" : ["Id","Code","Description","Number"],
        "Departments" : ["Id","Code","Description"],
    },

    "inventory": {
        "PartCodes" : ["Id","Alias","Code","Description"],
        "Parts" : ["Id","PartNumber","Description","ActiveRevisionId","Alias","AveragePrice","CompanyId","PartCodeId","PreparationId","StandardPrice","ExtraDescription","Type"],
        "StockTransactions" : ["Id","PartNumber","PartId","BalanceChange","BalanceOnPartAfterChange","LoggingTimeStamp","DeliveryDate","WarehouseId","OrderNumber","StandardPrice","Price","ReportedPrice","Amount"]
    },

    "manufacturing" : {
        "Drawings" : ["Id","DrawingNumber","PartId"],
        "ManualOperationStatuses" : ["Id","Code","Description"],
        "ManufacturingOrderMaterials" : ["Id","PartId","ReportedQuantity","ManufacturingOrderId"],
        "ManufacturingOrderNodeDrawings" : [],
        "ManufacturingOrderNodes" : ["Id","PartId","ReportedQuantity","PlannedQuantity","RestQuantity","StartDate","EndDate","WarehouseId","ManufacturingOrderNodeId"],
        "ManufacturingOrderOperationControlDataRows" : [],
        "ManufacturingOrderOperationReportings" : ["Id","OperationId","Quantity","SetupTime","UnitTime","WorkCenterId","WarehouseId","EmployeeId","ActualReportedDate",
                                                        "PreviousRestQuantity","RestQuantity","MainPartId","UnitCost","Type","SetupCost","ReportingTimeStamp","CommentId"],
        "ManufacturingOrderOperations" : ["Id","Priority","WorkCenterId","PartId","PlannedQuantity","ReportedQuantity",
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
                                     "ParentOrderId","ParentRowId","Price","StandardPrice","PriceInCompanyCurrency","RevisionId","NewFinishDate","UnitId"],
        "CustomerOrders" : ["Id","CustomerId","InvoiceCustomerId","OrderDate","OrderNumber","OrderTypeId","Status","SellerId"],
        "CustomerOrderTypes" : ["Id","Description","IncludeInStatistics"],
        "Customers" : ["Id","Name","Code","AlternativeName"],
        "InvoiceLogs" : ["Id","InvoiceId","InvoiceDate","PartId","PartNumber","PartCode","CustomerId","CustomerCode","CustomerOrderId","OrderedQuantity",
        "InvoicedQuantity","UnitCode","Price","PriceInCompanyCurrency","StandardPrice",
        "SetupPrice","Discount","ExchangeRate","OrderRowType","ConversionFactor","FifoPrice","DeliveryRowId"],
        "CustomerOrderInvoiceRows" : ["Id","OverrideStandardPrice","StandardPrice"],
        "CustomerOrderDeliveryRows" : ["Id","InvoiceRowId","CustomerOrderRowId"],
        "OrderQuantityCustomerOrders" : [],
        "SalesPrices" : [],
    },
    "purchase" : {
        "PurchaseOrderTypes" : [
            "Id","Number","Description","Priority"
        ],
        "PurchaseOrders" : [
            "Id","WarehouseId","Priority","ProjectId","OrderAmount","OrderAmountInCompanyCurrency","IsStockOrder","IsConfirmed",
            "CustomerOrderId","OrderNumber","OrderDate", "Rows","Status","IsCredit","OrderTypeId", "OrderType", "CategoryString",
        ],
        "PurchaseOrderRows" : [
            "Id","Position","OrderRowType","PartId","ParentRowId","DeliveryDate","RevisionId","OrderedQuantity","Price","Discount","ToolId","UnitId","WarehouseId","OrderDate",
            "PriceInCompanyCurrency","SetupPriceInCompanyCurrency","StandardPrice","SetupPrice","PartStatus","OrderedQuantityInOrderUnit","RestQuantity","DeliveredQuantity",
            "InitialDeliveryDate","DesiredDeliveryDate","IsConfirmed","LinkedStockOrderRowId","LinkedSubcontractingManufacturingOrderOperation","OriginatingPurchaseOrderDeliveryRowId",
            "SubPartParentRowId","QuantityRatio","ParentOrderId","RowIndex","SumRowId","PriceOrigin","OrderQuantityId","OrderQuantity",
        ],
        
        "PurchaseOrderInvoices":[
            "Id","WarehouseId","Rows"
        ],
        "PurchaseOrderInvoiceRows" : [
            "Id","PurchaseOrderDeliveryRowId","Price","SetupPrice","VatRateId","Discount","StandardPrice",
        ],
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
    "OperationRows" : ["Id","OrderOperation","UnitTime"],
    ""
    "ExtraFieldGroups" : ["Id","Name","RowNumber"],
    "ExtraFieldOptionTemplates" : ["Id","Code","Description","ExtraFieldTemplateId"],
    "ExtraFields" : ["Id","ParentId","ParentClass","SelectedOptionId","SelectedOptionIds"],
    "ExtraFieldTemplates" : ["Id","Name","ParentId","RowNumber"],
}

combine_reporting_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","OrderTypeId","CustomerOrderId","CustomerId","InitialFinishDate","Status","PartId",],
    "ManufacturingOrderOperationReportings" : ["Id","OperationId","Quantity","SetupTime","UnitTime","WarehouseId","EmployeeId","ActualReportedDate",
                                                        "PreviousRestQuantity","RestQuantity","MainPartId","UnitCost","SetupCost","Type","ReportingTimeStamp","CommentId"],
    "ManufacturingOrderOperations" : ["Id","WorkCenterId",
    "PlannedQuantity","ReportedQuantity","RejectedQuantity",
    "ReportedUnitTime","PlannedUnitTime","ReportedSetupTime","PlannedSetupTime",
    "ManufacturingOrderId","OperationNumber","OperationRowId","PlannedStartDate","ActualStartDate",
    "PlannedFinishDate","ActualFinishDate","WorkshopOperationStatus","Description"],
}

combine_inventory_tables = {
    "PartCodes" : ["Id","Alias","Code","Description"],
    "Parts" : ["Id","PartNumber","Description","ActiveRevisionId","Alias","AveragePrice","CompanyId","PartCodeId","StandardPrice","ExtraDescription"]
}

combine_purchase_tables = {
     "PurchaseOrders" : [
            "Id","WarehouseId","ProjectId","OrderAmount","OrderAmountInCompanyCurrency","IsStockOrder","IsConfirmed",
            "CustomerOrderId","OrderNumber","OrderDate", "Rows","Status","OrderTypeId","CategoryString",
            ],
        "PurchaseOrderRows" : [
            "Id","OrderRowType","PartId","ParentRowId","DeliveryDate","OrderedQuantity","Price","Discount","WarehouseId","OrderDate",
            "StandardPrice","SetupPrice","RestQuantity","DeliveredQuantity",
            "IsConfirmed","LinkedSubcontractingManufacturingOrderOperation",
            "SubPartParentRowId","ParentOrderId","PriceOrigin","OrderQuantityId","OrderQuantity",
            ],
}

combine_material_tables = {
    "InvoiceLogs": [
        "Id","PartId","CustomerCode",
        "InvoicedQuantity","Price","SetupPrice","Discount","ExchangeRate",
        "OrderRowType","ConversionFactor","DeliveryRowId",
        "StandardPrice","FifoPrice","InvoiceDate"
    ],
    "Parts": [
        "Id","PartNumber","Description","StandardPrice","Type"
    ],
    "Customers": [
        "Id","Code","Name"
    ],
    "ExtraFieldGroups" : [
        "Id","Name","RowNumber"
    ],
    "ExtraFieldOptionTemplates" : [
        "Id","Code","Description","ExtraFieldTemplateId"
    ],
    "ExtraFields" : [
        "Id","ParentId","ParentClass","SelectedOptionId","SelectedOptionIds"
    ],
    "ExtraFieldTemplates" : [
        "Id","Name","ParentId","RowNumber"
    ],
    "CustomerOrderInvoiceRows" : [
        "Id","OverrideStandardPrice","StandardPrice"
    ],
    "CustomerOrderDeliveryRows" : [
        "Id","InvoiceRowId","CustomerOrderRowId"
    ],
    "CustomerOrderRows" : [
        "Id","Price","StandardPrice"
    ],
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
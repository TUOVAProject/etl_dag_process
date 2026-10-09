#ENdpoint columns that are used to stage and combine raw data.
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
                                                        "PreviousRestQuantity","RestQuantity","MainPartId","UnitCost","Type","SetupCost","ReportingTimestamp","CommentId",
                                                        "SetupCostFactor1","SetupCostFactor2","SetupCostFactor3","UnitCostFactor1","UnitCostFactor2","UnitCostFactor3"],
        "ManufacturingOrderOperations" : ["Id","Priority","WorkCenterId","PartId","PlannedQuantity","ReportedQuantity",
                                                "RestQuantity","ReportedUnitTime",
                                                "ReportedSetupTime","RejectedQuantity","ManufacturingOrderId","OperationNumber","PlannedSetupTime","PlannedUnitTime",
                                                "PlannedStartDate","PlannedFinishDate","ActualStartDate","ActualFinishDate","ManufacturingOrderNodeId","Status",
                                                "OperationRowId","Description","FixedLeadTime"],
        "ManufacturingOrders" : ["Id","OrderNumber","Status","WarehouseId","PartId","ReportedQuantity","PlannedQuantity","RestQuantity","StartDate","FinishDate",
                                    "CustomerOrderId","CustomerId","InitialFinishDate","OrderTypeId"],
        "ManufacturingOrderTypes" : ["Id","Description"],
        "MaterialRows" : ["Id","PartId","Quantity","SetupQuantity"],
        "OperationRowControlDataRows" : [],
        "OperationRows" : ["Id","OperationNumber","Description","WorkCenterId","IsSubcontractor","SetupTime","UnitTime","QueueTime","UnitCost","SetupCost","FixedLeadTime"],
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
                                     "ParentOrderId","ParentRowId","Price","StandardPrice","PriceInCompanyCurrency","RevisionId","NewFinishDate","UnitId","PartStatus","Status"],
        "CustomerOrders" : ["Id","CustomerId","InvoiceCustomerId","OrderDate","OrderNumber","OrderTypeId","Status","SellerId"],
        "CustomerOrderTypes" : ["Id","Description","IncludeInStatistics"],
        "Customers" : ["Id","Name","Code","AlternativeName"],
        "InvoiceLogs" : ["Id","InvoiceId","InvoiceDate","PartId","PartNumber","PartCode","CustomerId","CustomerCode","CustomerOrderId","OrderedQuantity",
        "InvoicedQuantity","LoggingTimestamp","UnitCode","Price","PriceInCompanyCurrency","StandardPrice",
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
    },
    "time_recording" : {
        "WorkIntervals" : ["Id","IntervalStart","IntervalEnd","IsClosedInterval", "BundleId","WorkRecordingType","IndirectWorkCodeId", "OperationId",
          "TimeCalculationType","ReportingEmployeeId", "WorkCenterId","WorkedTime",
          "OfWichOvertime","ReportedQuantity","RejectedQuantity", "ActivityId", "CommentId", "Comment"
          ],
        "IndirectWorkCodes" : [
            "Id","Code","Description","CommentInputType","AffectsOrder","ColorCode","Active"
        ]
  
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
                                     "ParentOrderId","ParentRowId","Price","PriceInCompanyCurrency","RevisionId","NewFinishDate","UnitId","PartStatus","Status"],
    "CustomerOrders" : ["Id","CustomerId","InvoiceCustomerId","OrderDate","OrderNumber","OrderTypeId","Status","SellerId"],
    "Customers" : ["Id","Name","Code","AlternativeName"],
    "InvoiceLogs" : ["Id","CustomerOrderId","InvoicedQuantity","Price","LoggingTimestamp","InvoiceDate"],
    "ManufacturingOrders" : ["Id","OrderNumber","StartDate","InitialFinishDate","FinishDate","CustomerOrderId","Status"],
    "ManufacturingOrderOperations" : ["Id","ManufacturingOrderId","ActualStartDate","ActualFinishDate"],
}

combine_manufacturing_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","OrderTypeId","CustomerOrderId","CustomerId","InitialFinishDate","Status","PartId","ReportedQuantity","PlannedQuantity",],
    "ManufacturingOrderOperations" : ["Id","WorkCenterId","PartId",
        "PlannedQuantity","ReportedQuantity","RejectedQuantity","RestQuantity",
        "ReportedUnitTime","PlannedUnitTime","ReportedSetupTime","PlannedSetupTime",
        "ManufacturingOrderId","OperationNumber","OperationRowId","PlannedStartDate","ActualStartDate",
        "PlannedFinishDate","ActualFinishDate","WorkshopOperationStatus","Description","FixedLeadTime"],
    "OperationRows" : ["Id","OrderOperation","UnitTime","IsSubcontractor","UnitCost","SetupCost","FixedLeadTime"],
    "ExtraFieldGroups" : ["Id","Name","RowNumber"],
    "ExtraFieldOptionTemplates" : ["Id","Code","Description","ExtraFieldTemplateId"],
    "ExtraFields" : ["Id","ParentId","ParentClass","SelectedOptionId","SelectedOptionIds"],
    "ExtraFieldTemplates" : ["Id","Name","ParentId","RowNumber"],
    "WorkCenters" : ["Id","Number"],
    "CustomerOrderRows" : ["Id","LinkedManufacturingOrderId"],
}

combine_reporting_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","OrderTypeId","CustomerOrderId","CustomerId","InitialFinishDate","Status","PartId",],
    "ManufacturingOrderOperationReportings" : ["Id","OperationId","Quantity","SetupTime","UnitTime","WarehouseId","EmployeeId","ActualReportedDate",
        "PreviousRestQuantity","RestQuantity","MainPartId","UnitCost","SetupCost","Type","ReportingTimestamp","CommentId","SetupCostFactor1",
        "SetupCostFactor2","SetupCostFactor3","UnitCostFactor1","UnitCostFactor2","UnitCostFactor3"],
    "ManufacturingOrderOperations" : ["Id","WorkCenterId",
    "PlannedQuantity","ReportedQuantity","RejectedQuantity",
    "ReportedUnitTime","PlannedUnitTime","ReportedSetupTime","PlannedSetupTime",
    "ManufacturingOrderId","OperationNumber","OperationRowId","PlannedStartDate","ActualStartDate",
    "PlannedFinishDate","ActualFinishDate","WorkshopOperationStatus","Description"],
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
    "Customers" : ["Id","Code"],
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

combine_work_interval_tables = {
    "ManufacturingOrders" : ["Id","OrderNumber","PartId"],
    "ManufacturingOrderOperations" : ["Id","OrderNumber","ManufacturingOrderId","WorkCenterId","PartId"],
    "WorkIntervals" : ["Id","OperationId","ReportingEmployeeId","IntervalStart","IntervalEnd","WorkedTime","ReportedQuantity","RejectedQuantity"],
}


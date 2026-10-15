
from src.dag_test import erp_file_processor_test
from src.dataset_to_excel import dataset_to_excel,export_endpoint_to_csv,combined_dataset_to_excel,combined_export_endpoint_to_csv
from src.config.logger_config import get_custom_logger
<<<<<<< HEAD

def main():
    get_custom_logger()
    erp_file_processor_test()
    dataset_to_excel()
    combined_dataset_to_excel()
    
=======
from time import perf_counter


def main():
    get_custom_logger()
    
    erp_file_processor_test()
    
    dataset_to_excel()
    
    combined_dataset_to_excel()
    

>>>>>>> f2c804d093af96cd1e6f41a19581cea65a660872

if __name__ == "__main__":
    main()
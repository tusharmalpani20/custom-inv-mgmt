import frappe
import time

def comprehensive_data_processing_cron():
    """
    Simple comprehensive cron function that runs all data processing functions in logical order.
    
    Logical Flow:
    1. Create addresses from lat/long for SF Facility Master records without shipping_address
    2. Create warehouses for darkstore facilities (depends on addresses being available)
    3. Link darkstore addresses to internal customer (depends on warehouses being created)
    4. Process new customers from orders (external mappings → customers → addresses)
    5. Daily order aggregation (final step)
    """
    start_time = time.time()
    
    print("Starting comprehensive data processing cron...")
    
    try:
        # Step 1: Create addresses from lat/long
        print("Step 1: Creating addresses from latitude/longitude...")
        from inv_mgmt.cron_functions.create_address_from_lat_long import create_address_from_lat_long_for_sf_facility_master
        result1 = create_address_from_lat_long_for_sf_facility_master()
        print(f"Step 1 completed: {result1}")
        
        # Step 2: Create warehouses for darkstore
        print("Step 2: Creating warehouses for darkstore facilities...")
        from inv_mgmt.cron_functions.create_warehouse_from_sf_facility_master import create_missing_darkstore_warehouses
        result2 = create_missing_darkstore_warehouses()
        print(f"Step 2 completed: {result2}")
        
        # Step 3: Link darkstore addresses to internal customer
        print("Step 3: Linking darkstore addresses to internal customer...")
        from inv_mgmt.cron_functions.add_darkstore_address_to_internal_customer import link_darkstore_addresses_to_internal_customer
        result3 = link_darkstore_addresses_to_internal_customer()
        print(f"Step 3 completed: {result3}")
        
        # Step 4: Process new customers from orders
        print("Step 4: Processing new customers from orders...")
        from inv_mgmt.cron_functions.new_customers_from_orders import (
            run_new_customers_from_orders,
            run_create_customers_from_external_mappings,
            run_create_addresses_for_b2b_customers
        )
        
        print("Step 4a: Creating external mappings...")
        result4a = run_new_customers_from_orders()
        print(f"Step 4a completed: {result4a}")
        
        print("Step 4b: Creating customers from external mappings...")
        result4b = run_create_customers_from_external_mappings()
        print(f"Step 4b completed: {result4b}")
        
        print("Step 4c: Creating addresses for B2B customers...")
        result4c = run_create_addresses_for_b2b_customers()
        print(f"Step 4c completed: {result4c}")
        
        # Step 5: Daily order aggregation
        print("Step 5: Running daily order aggregation...")
        from inv_mgmt.cron_functions.aggregate_order_data import daily_order_aggregation
        result5 = daily_order_aggregation()
        print(f"Step 5 completed: {result5}")
        
        total_time = time.time() - start_time
        print(f"✅ Comprehensive data processing completed successfully in {total_time:.2f} seconds")
        
        return {
            "status": "success",
            "message": "All steps completed successfully",
            "total_time": total_time
        }
        
    except Exception as e:
        total_time = time.time() - start_time
        error_msg = f"Error in comprehensive cron: {str(e)}"
        print(f"❌ {error_msg}")
        
        return {
            "status": "error",
            "message": error_msg,
            "total_time": total_time,
            "error_details": {
                "exception_type": type(e).__name__,
                "error_message": str(e)
            }
        }

def enqueue_comprehensive_data_processing_cron():
    """
    Wrapper function to enqueue the comprehensive_data_processing_cron job with extended timeout
    """
    try:
        frappe.enqueue(
            method="inv_mgmt.cron_functions.comprehensive_data_processing_cron.comprehensive_data_processing_cron",
            queue="long",
            timeout=1500,  # 25 minutes timeout
            job_name="comprehensive_data_processing_cron",
            user="Administrator",
            is_async=True
        )
        print("Comprehensive data processing cron job has been enqueued successfully")
        return {"status": "success", "message": "Job enqueued successfully"}
    except Exception as e:
        print(f"Error enqueueing comprehensive_data_processing_cron job: {str(e)}")
        return {"status": "error", "message": str(e)}

# For manual execution
# bench execute "inv_mgmt.cron_functions.comprehensive_data_processing_cron.comprehensive_data_processing_cron" 
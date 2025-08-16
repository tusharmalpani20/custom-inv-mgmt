import frappe
from frappe import _
from frappe.utils import getdate, nowdate, add_days
import json
from typing import List, Dict, Any, Optional
from collections import defaultdict

# Debug flag - set to True to enable debug print statements
DEBUG = True

def debug_print(message: str):
    """Print debug messages only when DEBUG flag is True"""
    if DEBUG:
        print(f"[DEBUG] {message}")

def error_print(message: str):
    """Print error messages - always visible"""
    print(f"[ERROR] {message}")

def info_print(message: str):
    """Print info messages - always visible"""
    print(f"[INFO] {message}")


def log_error(error_category: str, error_description: str, processing_stage: str, 
              entity_type: str = None, external_id: str = None, 
              reference_doctype: str = None, internal_reference: str = None,
              error_severity: str = "Medium", additional_detail: Dict = None):
    """
    Log errors to SF Inventory Data Import Error Logs
    
    Args:
        error_category: Category of the error
        error_description: Description of the error
        processing_stage: Stage where error occurred
        entity_type: Type of entity being processed
        external_id: External ID if applicable
        reference_doctype: Reference doctype if applicable
        internal_reference: Internal reference if applicable
        error_severity: Severity level of the error
        additional_detail: Additional details as JSON
    """
    try:
        error_log = frappe.get_doc({
            "doctype": "SF Inventory Data Import Error Logs",
            "error_category": error_category,
            "error_description": error_description,
            "processing_stage": processing_stage,
            "entity_type": entity_type,
            "external_id": external_id,
            "reference_doctype": reference_doctype,
            "internal_reference": internal_reference,
            "error_severity": error_severity,
            "additional_detail": json.dumps(additional_detail) if additional_detail else None,
            "error_date": nowdate(),
            "source_system": "Scheduled Jobs"
        })
        error_log.insert(ignore_permissions=True)
        
        # Only commit if we're not in a transaction to avoid implicit commit errors
        if not hasattr(frappe.db, 'transaction_writes') or frappe.db.transaction_writes == 0:
            frappe.db.commit()
            debug_print(f"Error logged: {error_description}")
        else:
            debug_print(f"Error logged (within transaction, will commit later): {error_description}")
    except Exception as e:
        error_print(f"Failed to log error: {str(e)}")


def aggregate_orders_and_create_sales_orders(branches: List[str], order_date: str) -> Dict[str, Any]:
    """
    Main function to aggregate orders from SF Order Master for specific branches and date,
    then create cyclic sales orders based on plant -> distribution center -> darkstore hierarchy.
    Handles both D2C and B2B orders with enhanced logic.
    
    Args:
        branches: List of branch names to process
        order_date: Date to fetch orders for (YYYY-MM-DD format)
    
    Returns:
        Dict with aggregation results and created sales orders
    """
    # Remove transaction management from this level - let sub-functions handle their own transactions
    # frappe.db.begin()  # REMOVED - this was causing the implicit commit error
    
    try:
        info_print(f"Starting order aggregation for branches: {branches}, date: {order_date}")
        
        # Step 1: Build warehouse hierarchy
        hierarchy = build_warehouse_hierarchy(branches)
        debug_print(f"Built warehouse hierarchy with {len(hierarchy)} plants")
        
        # Check if any plants were found
        if not hierarchy:
            info_print(f"No plants found for branches: {branches}")
            log_error(
                error_category="Data Validation",
                error_description=f"No plants found for specified branches: {branches}",
                processing_stage="Data Validation",
                entity_type="Warehouse",
                error_severity="Medium",
                additional_detail={
                    "branches": branches,
                    "order_date": order_date,
                    "message": "No plants with custom_warehouse_category = 'Plant' found for the specified branches"
                }
            )
            
            # Remove commit - let log_error handle its own transaction
            # frappe.db.commit()  # REMOVED
            return {
                "status": "success",
                "hierarchy": {},
                "total_d2c_orders_fetched": 0,
                "total_b2b_orders_fetched": 0,
                "total_orders_processed": 0,
                "d2c_processing_results": {"processed_orders": [], "discarded_orders": []},
                "b2b_processing_results": {"processed_orders": [], "discarded_orders": []},
                "grouped_orders_count": 0,
                "created_sales_orders": {
                    "d2c_orders": [],
                    "b2b_orders": [],
                    "total": 0
                },
                "message": f"No plants found for branches: {branches}. No orders to process."
            }
        
        # Step 2: Get D2C and B2B orders for the specified date and plants
        d2c_orders = get_d2c_orders_for_date_and_plants(order_date, list(hierarchy.keys()))
        b2b_orders = get_b2b_orders_for_date_and_plants(order_date, list(hierarchy.keys()))
        
        info_print(f"Fetched {len(d2c_orders)} D2C orders and {len(b2b_orders)} B2B orders")
        
        # Step 3: Group orders by hierarchy
        grouped_d2c_orders = group_orders_by_hierarchy(d2c_orders, hierarchy, "D2C")
        grouped_b2b_orders = group_orders_by_hierarchy(b2b_orders, hierarchy, "B2B")
        
        debug_print(f"Grouped orders by hierarchy")
        
        # Step 4: Process items and expand combos (validate items individually)
        processed_d2c_orders, d2c_processing_results = process_order_items(grouped_d2c_orders, "D2C")
        processed_b2b_orders, b2b_processing_results = process_order_items(grouped_b2b_orders, "B2B")
        
        total_processed_orders = len([o for plant in processed_d2c_orders.values() for dc in plant.values() for ds in dc.values() for o in ds.get('orders', [])]) + \
                               len([o for plant in processed_b2b_orders.values() for dc in plant.values() for ds in dc.values() for o in ds.get('orders', [])])
        
        info_print(f"Processed orders - D2C Valid: {len(d2c_processing_results['processed_orders'])}, B2B Valid: {len(b2b_processing_results['processed_orders'])}, Total Discarded: {len(d2c_processing_results['discarded_orders']) + len(b2b_processing_results['discarded_orders'])}")
        
        # Step 5: Create cyclic sales orders
        # Combine D2C and B2B for internal transfers, but separate for customer deliveries
        
        # Ensure any pending transactions from error logging are committed before sales order creation
        try:
            if hasattr(frappe.db, 'transaction_writes') and frappe.db.transaction_writes > 0:
                frappe.db.commit()
                debug_print("Committed pending transactions before sales order creation")
        except Exception as e:
            debug_print(f"No pending transactions to commit: {str(e)}")
        
        created_sales_orders = create_combined_sales_orders(
            processed_d2c_orders, processed_b2b_orders, order_date
        )
        
        total_created_orders = len(created_sales_orders)
        info_print(f"Created {total_created_orders} sales orders (D2C: {len(created_sales_orders['d2c_orders'])}, B2B: {len(created_sales_orders['b2b_orders'])})")
        
        # Step 6: Mark successfully processed orders and items
        mark_orders_and_items_as_processed(d2c_processing_results, b2b_processing_results)
        
        # Remove commit - let mark_orders_and_items_as_processed handle its own transaction
        # frappe.db.commit()  # REMOVED
        
        return {
            "status": "success",
            "hierarchy": hierarchy,
            "total_d2c_orders_fetched": len(d2c_orders),
            "total_b2b_orders_fetched": len(b2b_orders),
            "total_orders_processed": total_processed_orders,
            "d2c_processing_results": d2c_processing_results,
            "b2b_processing_results": b2b_processing_results,
            "grouped_orders_count": len(processed_d2c_orders) + len(processed_b2b_orders),
            "created_sales_orders": created_sales_orders,
            "message": f"Successfully processed {total_processed_orders} orders and created {total_created_orders} sales orders"
        }
        
    except Exception as e:
        # Remove rollback - no transaction was started at this level
        # frappe.db.rollback()  # REMOVED
        error_print(f"Error in aggregate_orders_and_create_sales_orders: {str(e)}")
        
        # Log the error
        log_error(
            error_category="System Error",
            error_description=f"Order aggregation failed: {str(e)}",
            processing_stage="Order Aggregation",
            entity_type="Order D2C",
            error_severity="Critical",
            additional_detail={
                "branches": branches,
                "order_date": order_date,
                "error": str(e)
            }
        )
        
        return {
            "status": "error",
            "message": str(e)
        }


def build_warehouse_hierarchy(branches: List[str]) -> Dict[str, Dict]:
    """
    Build the warehouse hierarchy: Plant -> Distribution Center -> Dark Store
    
    Args:
        branches: List of branch names
    
    Returns:
        Dict with plant as key and nested dict of distribution centers and darkstores
    """
    debug_print(f"Building warehouse hierarchy for branches: {branches}")
    hierarchy = {}
    
    try:
        # Debug: Check what warehouses exist for the specified branches
        all_warehouses = frappe.db.sql("""
            SELECT name, warehouse_name, custom_branch, custom_warehouse_category
            FROM `tabWarehouse`
            WHERE custom_branch IN %(branches)s
            AND disabled = 0
            ORDER BY custom_warehouse_category, name
        """, {"branches": branches}, as_dict=True)
        
        debug_print(f"Found {len(all_warehouses)} total warehouses for branches {branches}")
        for warehouse in all_warehouses:
            debug_print(f"  - {warehouse.name} ({warehouse.warehouse_name}) - Branch: {warehouse.custom_branch}, Category: {warehouse.custom_warehouse_category}")
        
        # Get all plants for the specified branches
        plants = frappe.db.sql("""
            SELECT name, warehouse_name, custom_branch
            FROM `tabWarehouse`
            WHERE custom_warehouse_category = 'Plant'
            AND custom_branch IN %(branches)s
            AND disabled = 0
        """, {"branches": branches}, as_dict=True)
        
        debug_print(f"Found {len(plants)} plants")
        
        for plant in plants:
            plant_facility = frappe.db.get_value(
                "SF Facility Master",
                {"warehouse": plant.name, "type": "Plant"},
                ["name", "facility_id", "facility_name", "shipping_address"],
                as_dict=True
            )
            
            if not plant_facility:
                debug_print(f"No facility found for plant {plant.name}")
                log_error(
                    error_category="Missing Reference",
                    error_description=f"No SF Facility Master record found for plant warehouse: {plant.name}",
                    processing_stage="Data Validation",
                    entity_type="Facility",
                    external_id=plant.name,
                    reference_doctype="Warehouse",
                    internal_reference=plant.name,
                    error_severity="High",
                    additional_detail={
                        "warehouse_name": plant.warehouse_name,
                        "branch": plant.custom_branch,
                        "warehouse_type": "Plant"
                    }
                )
                continue
                
            hierarchy[plant.name] = {
                "plant_info": {
                    "warehouse": plant.name,
                    "warehouse_name": plant.warehouse_name,
                    "branch": plant.custom_branch,
                    "facility": plant_facility
                },
                "distribution_centers": {}
            }
            
            # Get distribution centers linked to this plant
            distribution_centers = frappe.db.sql("""
                SELECT name, warehouse_name, custom_branch
                FROM `tabWarehouse`
                WHERE custom_warehouse_category = 'Distribution Center'
                AND custom_plant_link = %(plant)s
                AND disabled = 0
            """, {"plant": plant.name}, as_dict=True)
            
            debug_print(f"Found {len(distribution_centers)} distribution centers for plant {plant.name}")
            
            for dc in distribution_centers:
                hierarchy[plant.name]["distribution_centers"][dc.name] = {
                    "dc_info": {
                        "warehouse": dc.name,
                        "warehouse_name": dc.warehouse_name,
                        "branch": dc.custom_branch
                    },
                    "darkstores": {}
                }
                
                # Get darkstores linked to this distribution center
                darkstores = frappe.db.sql("""
                    SELECT name, warehouse_name, custom_branch
                    FROM `tabWarehouse`
                    WHERE custom_warehouse_category = 'Darkstore'
                    AND custom_distribution_center_link = %(dc)s
                    AND disabled = 0
                """, {"dc": dc.name}, as_dict=True)
                
                debug_print(f"Found {len(darkstores)} darkstores for DC {dc.name}")
                
                for darkstore in darkstores:
                    darkstore_facility = frappe.db.get_value(
                        "SF Facility Master",
                        {"warehouse": darkstore.name, "type": "Darkstore"},
                        ["name", "facility_id", "facility_name", "shipping_address"],
                        as_dict=True
                    )
                    
                    if darkstore_facility:
                        hierarchy[plant.name]["distribution_centers"][dc.name]["darkstores"][darkstore.name] = {
                            "darkstore_info": {
                                "warehouse": darkstore.name,
                                "warehouse_name": darkstore.warehouse_name,
                                "branch": darkstore.custom_branch,
                                "facility": darkstore_facility
                            }
                        }
                    else:
                        log_error(
                            error_category="Missing Reference",
                            error_description=f"No SF Facility Master record found for darkstore warehouse: {darkstore.name}",
                            processing_stage="Data Validation",
                            entity_type="Facility",
                            external_id=darkstore.name,
                            reference_doctype="Warehouse",
                            internal_reference=darkstore.name,
                            error_severity="Medium",
                            additional_detail={
                                "warehouse_name": darkstore.warehouse_name,
                                "branch": darkstore.custom_branch,
                                "warehouse_type": "Darkstore",
                                "parent_dc": dc.name,
                                "parent_plant": plant.name
                            }
                        )
        
        return hierarchy
        
    except Exception as e:
        error_print(f"Error building warehouse hierarchy: {str(e)}")
        log_error(
            error_category="System Error",
            error_description=f"Failed to build warehouse hierarchy: {str(e)}",
            processing_stage="Data Validation",
            entity_type="Warehouse",
            error_severity="Critical",
            additional_detail={
                "branches": branches,
                "error": str(e)
            }
        )
        raise


def get_d2c_orders_for_date_and_plants(order_date: str, plant_names: List[str]) -> List[Dict]:
    """
    Get D2C orders for specific date and plants that haven't been processed yet
    
    Args:
        order_date: Date in YYYY-MM-DD format
        plant_names: List of plant warehouse names
    
    Returns:
        List of order dictionaries
    """
    debug_print(f"Getting D2C orders for date {order_date} and {len(plant_names)} plants")
    
    # If no plants, return empty list
    if not plant_names:
        debug_print("No plants provided, returning empty order list")
        return []
    
    # Get SF Facility Master names for the plants
    plant_facilities = frappe.db.sql("""
        SELECT name
        FROM `tabSF Facility Master`
        WHERE warehouse IN %(plants)s
        AND type = 'Plant'
    """, {"plants": plant_names}, as_dict=True)
    
    if not plant_facilities:
        debug_print("No plant facilities found")
        return []
    
    plant_facility_names = [pf.name for pf in plant_facilities]
    debug_print(f"Found {len(plant_facility_names)} plant facilities")
    
    # Get D2C orders for the date and plants that haven't been processed
    orders = frappe.db.sql("""
        SELECT *
        FROM `tabSF Order Master`
        WHERE order_type = 'D2C'
        AND order_date = %(order_date)s
        AND plant IN %(plant_facilities)s
        AND (processing_status IS NULL OR processing_status = 'Unprocessed' OR processing_status = 'Partially Processed')
        ORDER BY plant, darkstore
    """, {
        "order_date": order_date,
        "plant_facilities": plant_facility_names
    }, as_dict=True)
    
    debug_print(f"Found {len(orders)} unprocessed D2C orders")
    return orders


def get_b2b_orders_for_date_and_plants(order_date: str, plant_names: List[str]) -> List[Dict]:
    """
    Get B2B orders for specific date and plants that haven't been processed yet
    
    Args:
        order_date: Date in YYYY-MM-DD format
        plant_names: List of plant warehouse names
    
    Returns:
        List of order dictionaries
    """
    debug_print(f"Getting B2B orders for date {order_date} and {len(plant_names)} plants")
    
    # If no plants, return empty list
    if not plant_names:
        debug_print("No plants provided, returning empty order list")
        return []
    
    # Get SF Facility Master names for the plants
    plant_facilities = frappe.db.sql("""
        SELECT name
        FROM `tabSF Facility Master`
        WHERE warehouse IN %(plants)s
        AND type = 'Plant'
    """, {"plants": plant_names}, as_dict=True)
    
    if not plant_facilities:
        debug_print("No plant facilities found")
        return []
    
    plant_facility_names = [pf.name for pf in plant_facilities]
    debug_print(f"Found {len(plant_facility_names)} plant facilities")
    
    # Get B2B orders for the date and plants that haven't been processed
    orders = frappe.db.sql("""
        SELECT *
        FROM `tabSF Order Master`
        WHERE order_type = 'B2B'
        AND order_date = %(order_date)s
        AND plant IN %(plant_facilities)s
        AND (processing_status IS NULL OR processing_status = 'Unprocessed' OR processing_status = 'Partially Processed')
        ORDER BY plant, darkstore
    """, {
        "order_date": order_date,
        "plant_facilities": plant_facility_names
    }, as_dict=True)
    
    debug_print(f"Found {len(orders)} unprocessed B2B orders")
    return orders


def group_orders_by_hierarchy(orders: List[Dict], hierarchy: Dict, order_type: str) -> Dict:
    """
    Group orders by hierarchy with enhanced logic for distribution centers
    
    Args:
        orders: List of SF Order Master records
        hierarchy: Warehouse hierarchy dict
        order_type: "D2C" or "B2B"
    
    Returns:
        Grouped orders by hierarchy
    """
    debug_print(f"Grouping {len(orders)} {order_type} orders by hierarchy")
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    # Create reverse mapping from facility to warehouse
    facility_to_warehouse = {}
    for plant_warehouse, plant_data in hierarchy.items():
        if plant_data["plant_info"]["facility"]:
            facility_to_warehouse[plant_data["plant_info"]["facility"]["name"]] = {
                "type": "plant",
                "warehouse": plant_warehouse
            }
        
        for dc_warehouse, dc_data in plant_data["distribution_centers"].items():
            for darkstore_warehouse, darkstore_data in dc_data["darkstores"].items():
                if darkstore_data["darkstore_info"]["facility"]:
                    facility_to_warehouse[darkstore_data["darkstore_info"]["facility"]["name"]] = {
                        "type": "darkstore",
                        "warehouse": darkstore_warehouse,
                        "plant": plant_warehouse,
                        "dc": dc_warehouse
                    }
    
    # Also add distribution centers from SF Facility Master
    dc_facilities = frappe.db.sql("""
        SELECT name, warehouse, type
        FROM `tabSF Facility Master`
        WHERE type = 'Distribution Center'
        AND warehouse IN (
            SELECT name FROM `tabWarehouse` 
            WHERE custom_warehouse_category = 'Distribution Center'
        )
    """, as_dict=True)
    
    for dc_facility in dc_facilities:
        dc_warehouse = dc_facility.warehouse
        # Find which plant this DC belongs to
        plant_warehouse = frappe.db.get_value("Warehouse", dc_warehouse, "custom_plant_link")
        if plant_warehouse and plant_warehouse in hierarchy:
            facility_to_warehouse[dc_facility.name] = {
                "type": "distribution_center",
                "warehouse": dc_warehouse,
                "plant": plant_warehouse,
                "dc": dc_warehouse  # Same as warehouse for DC
            }
    
    debug_print(f"Created facility to warehouse mapping with {len(facility_to_warehouse)} entries")
    
    grouped_count = 0
    for order in orders:
        plant_facility = order.get("plant")
        darkstore_facility = order.get("darkstore")
        
        if not plant_facility:
            debug_print(f"Skipping order {order.get('name')} - missing plant facility")
            continue
            
        # Find the corresponding plant warehouse
        plant_info = facility_to_warehouse.get(plant_facility)
        if not plant_info:
            debug_print(f"Skipping order {order.get('name')} - plant facility mapping not found")
            continue
            
        plant_warehouse = plant_info["warehouse"]
        
        if order_type == "D2C":
            # D2C orders must have a darkstore/distribution center
            if not darkstore_facility:
                debug_print(f"Skipping D2C order {order.get('name')} - missing darkstore facility")
                continue
                
            darkstore_info = facility_to_warehouse.get(darkstore_facility)
            if not darkstore_info:
                debug_print(f"Skipping D2C order {order.get('name')} - darkstore facility mapping not found")
                continue
                
            if darkstore_info["type"] == "darkstore":
                # Regular darkstore order
                darkstore_warehouse = darkstore_info["warehouse"]
                dc_warehouse = darkstore_info["dc"]
                grouped[plant_warehouse][dc_warehouse][darkstore_warehouse].append(order)
                grouped_count += 1
            elif darkstore_info["type"] == "distribution_center":
                # Distribution center order - group directly under DC
                dc_warehouse = darkstore_info["warehouse"]
                grouped[plant_warehouse][dc_warehouse]["DC_DIRECT"].append(order)
                grouped_count += 1
                
        elif order_type == "B2B":
            # B2B orders can have different scenarios
            if not darkstore_facility:
                # Direct plant to client
                grouped[plant_warehouse]["DIRECT"]["CLIENT"].append(order)
                grouped_count += 1
            else:
                darkstore_info = facility_to_warehouse.get(darkstore_facility)
                if not darkstore_info:
                    debug_print(f"Skipping B2B order {order.get('name')} - darkstore facility mapping not found")
                    continue
                    
                if darkstore_info["type"] == "darkstore":
                    # Plant -> DC -> Darkstore -> Client
                    darkstore_warehouse = darkstore_info["warehouse"]
                    dc_warehouse = darkstore_info["dc"]
                    grouped[plant_warehouse][dc_warehouse][darkstore_warehouse].append(order)
                    grouped_count += 1
                elif darkstore_info["type"] == "distribution_center":
                    # Plant -> DC -> Client
                    dc_warehouse = darkstore_info["warehouse"]
                    grouped[plant_warehouse][dc_warehouse]["DC_DIRECT"].append(order)
                    grouped_count += 1
    
    debug_print(f"Successfully grouped {grouped_count} {order_type} orders")
    return dict(grouped)


def process_order_items(grouped_orders: Dict, order_type: str) -> tuple[Dict, Dict]:
    """
    Process order items and expand combo items to individual items.
    Process items individually and track which items are successfully processed.
    
    Args:
        grouped_orders: Orders grouped by hierarchy
        order_type: "D2C" or "B2B"
    
    Returns:
        Tuple of (processed_orders, processing_results)
    """
    debug_print(f"Processing {order_type} order items and validating product links")
    processed = {}
    processing_results = {
        "processed_orders": [],
        "discarded_orders": [],
        "item_processing_details": {}  # order_name -> {item_id -> success/failure}
    }
    
    try:
        for plant, plant_data in grouped_orders.items():
            processed[plant] = {}
            for dc, dc_data in plant_data.items():
                processed[plant][dc] = {}
                for darkstore, orders in dc_data.items():
                    processed[plant][dc][darkstore] = {
                        "orders": [],
                        "items": defaultdict(float)  # item_code -> total_quantity
                    }
                    
                    debug_print(f"Processing {len(orders)} {order_type} orders for darkstore {darkstore}")
                    
                    # Process each order's items
                    for order in orders:
                        order_items_processed = {}
                        order_processing_details = {}
                        
                        order_items = frappe.db.sql("""
                            SELECT item_id, item_name, quantity, sf_product_master, name as item_row_name
                            FROM `tabSF Order Item`
                            WHERE parent = %(order_name)s
                            AND (is_item_processed IS NULL OR is_item_processed = 0)
                        """, {"order_name": order.name}, as_dict=True)
                        
                        # Process each item individually
                        for order_item in order_items:
                            item_success = False
                            item_processed_items = {}
                            
                            try:
                                if not order_item.sf_product_master:
                                    error_print(f"Order {order.name}: SF Product Master not found for item {order_item.item_id}")
                                    log_error(
                                        error_category="Missing Reference",
                                        error_description=f"SF Product Master not found for item {order_item.item_id}",
                                        processing_stage="Item Validation",
                                        entity_type="Product",
                                        external_id=order_item.item_id,
                                        reference_doctype="SF Order Item",
                                        internal_reference=order_item.item_row_name,
                                        error_severity="High",
                                        additional_detail={
                                            "order_name": order.name,
                                            "order_id": order.order_id,
                                            "item_name": order_item.item_name,
                                            "quantity": order_item.quantity
                                        }
                                    )
                                    order_processing_details[order_item.item_id] = {
                                        "success": False,
                                        "reason": "SF Product Master not found",
                                        "item_row_name": order_item.item_row_name
                                    }
                                    continue
                                    
                                sf_product = frappe.get_doc("SF Product Master", order_item.sf_product_master)
                                
                                if sf_product.is_combo:
                                    # Validate and expand combo items
                                    combo_items = validate_and_expand_combo_items(sf_product, order_item.quantity, order.name)
                                    if combo_items is None:  # Validation failed
                                        order_processing_details[order_item.item_id] = {
                                            "success": False,
                                            "reason": "Combo item validation failed",
                                            "item_row_name": order_item.item_row_name
                                        }
                                        continue
                                    item_processed_items = combo_items
                                    item_success = True
                                else:
                                    # Validate single item
                                    if not sf_product.item_link:
                                        error_print(f"Order {order.name}: SF Product {sf_product.name} has no item_link to ERPNext Item")
                                        log_error(
                                            error_category="Product Linking",
                                            error_description=f"SF Product {sf_product.name} has no ERPNext item link",
                                            processing_stage="Item Validation",
                                            entity_type="Product",
                                            external_id=sf_product.sf_product_id,
                                            reference_doctype="SF Product Master",
                                            internal_reference=sf_product.name,
                                            error_severity="High",
                                            additional_detail={
                                                "order_name": order.name,
                                                "order_id": order.order_id,
                                                "sf_product_name": sf_product.name,
                                                "sf_product_id": sf_product.sf_product_id
                                            }
                                        )
                                        order_processing_details[order_item.item_id] = {
                                            "success": False,
                                            "reason": "No ERPNext item link",
                                            "item_row_name": order_item.item_row_name
                                        }
                                        continue
                                        
                                    # Check if the linked item exists in ERPNext
                                    if not frappe.db.exists("Item", sf_product.item_link):
                                        error_print(f"Order {order.name}: ERPNext Item {sf_product.item_link} does not exist")
                                        log_error(
                                            error_category="Missing Reference",
                                            error_description=f"ERPNext Item {sf_product.item_link} does not exist",
                                            processing_stage="Item Validation",
                                            entity_type="Item",
                                            external_id=sf_product.item_link,
                                            reference_doctype="Item",
                                            internal_reference=sf_product.item_link,
                                            error_severity="High",
                                            additional_detail={
                                                "order_name": order.name,
                                                "order_id": order.order_id,
                                                "sf_product_name": sf_product.name,
                                                "sf_product_id": sf_product.sf_product_id,
                                                "erpnext_item": sf_product.item_link
                                            }
                                        )
                                        order_processing_details[order_item.item_id] = {
                                            "success": False,
                                            "reason": "ERPNext item does not exist",
                                            "item_row_name": order_item.item_row_name
                                        }
                                        continue
                                        
                                    item_processed_items = {sf_product.item_link: order_item.quantity}
                                    item_success = True
                                
                                # If item was successfully processed, add to aggregated items
                                if item_success:
                                    for item_code, qty in item_processed_items.items():
                                        processed[plant][dc][darkstore]["items"][item_code] += qty
                                    
                                    order_processing_details[order_item.item_id] = {
                                        "success": True,
                                        "processed_items": item_processed_items,
                                        "item_row_name": order_item.item_row_name
                                    }
                                    
                            except Exception as e:
                                error_print(f"Error processing item {order_item.item_id} in order {order.name}: {str(e)}")
                                log_error(
                                    error_category="System Error",
                                    error_description=f"Error processing item {order_item.item_id}: {str(e)}",
                                    processing_stage="Item Validation",
                                    entity_type="Product",
                                    external_id=order_item.item_id,
                                    reference_doctype="SF Order Item",
                                    internal_reference=order_item.item_row_name,
                                    error_severity="Medium",
                                    additional_detail={
                                        "order_name": order.name,
                                        "order_id": order.order_id,
                                        "item_name": order_item.item_name,
                                        "quantity": order_item.quantity,
                                        "error": str(e)
                                    }
                                )
                                order_processing_details[order_item.item_id] = {
                                    "success": False,
                                    "reason": f"Processing error: {str(e)}",
                                    "item_row_name": order_item.item_row_name
                                }
                        
                        # Store processing details for this order
                        processing_results["item_processing_details"][order.name] = order_processing_details
                        
                        # Check if any items were successfully processed
                        successful_items = [details for details in order_processing_details.values() if details["success"]]
                        total_items = len(order_processing_details)
                        
                        if successful_items:
                            # Add order to processed list if at least one item was successful
                            processed[plant][dc][darkstore]["orders"].append(order)
                            processing_results["processed_orders"].append({
                                "order_name": order.name,
                                "order_id": order.order_id,
                                "successful_items": len(successful_items),
                                "total_items": total_items
                            })
                            debug_print(f"Order {order.name} processed successfully - {len(successful_items)}/{total_items} items")
                        else:
                            # No items were successful
                            processing_results["discarded_orders"].append({
                                "order_name": order.name,
                                "order_id": order.order_id,
                                "reason": "No items could be processed",
                                "total_items": total_items
                            })
                            error_print(f"Order {order.name} discarded - no items could be processed")
                            log_error(
                                error_category="Order Processing",
                                error_description=f"Order {order.name} discarded - no items could be processed",
                                processing_stage="Business Logic",
                                entity_type=f"Order {order_type}",
                                external_id=order.order_id,
                                reference_doctype="SF Order Master",
                                internal_reference=order.name,
                                error_severity="Medium",
                                additional_detail={
                                    "order_name": order.name,
                                    "order_id": order.order_id,
                                    "total_items": total_items,
                                    "order_type": order_type
                                }
                            )
                    
                    # Remove empty darkstore entries
                    if not processed[plant][dc][darkstore]["orders"]:
                        del processed[plant][dc][darkstore]
                
                # Remove empty DC entries
                if not processed[plant][dc]:
                    del processed[plant][dc]
            
            # Remove empty plant entries
            if not processed[plant]:
                del processed[plant]
        
        info_print(f"{order_type} order processing complete - {len(processing_results['discarded_orders'])} orders discarded")
        return processed, processing_results
        
    except Exception as e:
        error_print(f"Error in process_order_items for {order_type}: {str(e)}")
        log_error(
            error_category="System Error",
            error_description=f"Error processing {order_type} order items: {str(e)}",
            processing_stage="Business Logic",
            entity_type=f"Order {order_type}",
            error_severity="Critical",
            additional_detail={
                "order_type": order_type,
                "error": str(e)
            }
        )
        raise


def validate_and_expand_combo_items(sf_product: object, order_quantity: float, order_name: str) -> Optional[Dict[str, float]]:
    """
    Validate and expand combo items to individual items
    
    Args:
        sf_product: SF Product Master document
        order_quantity: Quantity ordered
        order_name: Order name for error logging
    
    Returns:
        Dict of item_code -> total_quantity, or None if validation fails
    """
    debug_print(f"Validating combo product {sf_product.name} for order {order_name}")
    expanded_items = {}
    
    try:
        if not sf_product.combo_items:
            error_print(f"Order {order_name}: Combo product {sf_product.name} has no combo items defined")
            log_error(
                error_category="Combo Item Validation",
                error_description=f"Combo product {sf_product.name} has no combo items defined",
                processing_stage="Item Validation",
                entity_type="Combo Product",
                external_id=sf_product.sf_product_id,
                reference_doctype="SF Product Master",
                internal_reference=sf_product.name,
                error_severity="High",
                additional_detail={
                    "order_name": order_name,
                    "sf_product_name": sf_product.name,
                    "sf_product_id": sf_product.sf_product_id,
                    "order_quantity": order_quantity
                }
            )
            return None
        
        for combo_item in sf_product.combo_items:
            try:
                combo_sf_product = frappe.get_doc("SF Product Master", combo_item.sf_product_id)
            except frappe.DoesNotExistError:
                error_print(f"Order {order_name}: Combo item SF Product {combo_item.sf_product_id} does not exist")
                log_error(
                    error_category="Missing Reference",
                    error_description=f"Combo item SF Product {combo_item.sf_product_id} does not exist",
                    processing_stage="Item Validation",
                    entity_type="Product",
                    external_id=combo_item.sf_product_id,
                    reference_doctype="SF Product Master",
                    internal_reference=combo_item.sf_product_id,
                    error_severity="High",
                    additional_detail={
                        "order_name": order_name,
                        "parent_sf_product": sf_product.name,
                        "parent_sf_product_id": sf_product.sf_product_id,
                        "combo_item_id": combo_item.sf_product_id,
                        "combo_quantity": combo_item.quantity,
                        "order_quantity": order_quantity
                    }
                )
                return None
                
            if not combo_sf_product.item_link:
                error_print(f"Order {order_name}: Combo item SF Product {combo_sf_product.name} has no item_link to ERPNext Item")
                log_error(
                    error_category="Product Linking",
                    error_description=f"Combo item SF Product {combo_sf_product.name} has no ERPNext item link",
                    processing_stage="Item Validation",
                    entity_type="Product",
                    external_id=combo_sf_product.sf_product_id,
                    reference_doctype="SF Product Master",
                    internal_reference=combo_sf_product.name,
                    error_severity="High",
                    additional_detail={
                        "order_name": order_name,
                        "parent_sf_product": sf_product.name,
                        "parent_sf_product_id": sf_product.sf_product_id,
                        "combo_sf_product_name": combo_sf_product.name,
                        "combo_sf_product_id": combo_sf_product.sf_product_id,
                        "combo_quantity": combo_item.quantity,
                        "order_quantity": order_quantity
                    }
                )
                return None
                
            # Check if the linked item exists in ERPNext
            if not frappe.db.exists("Item", combo_sf_product.item_link):
                error_print(f"Order {order_name}: ERPNext Item {combo_sf_product.item_link} does not exist")
                log_error(
                    error_category="Missing Reference",
                    error_description=f"ERPNext Item {combo_sf_product.item_link} does not exist for combo item",
                    processing_stage="Item Validation",
                    entity_type="Item",
                    external_id=combo_sf_product.item_link,
                    reference_doctype="Item",
                    internal_reference=combo_sf_product.item_link,
                    error_severity="High",
                    additional_detail={
                        "order_name": order_name,
                        "parent_sf_product": sf_product.name,
                        "parent_sf_product_id": sf_product.sf_product_id,
                        "combo_sf_product_name": combo_sf_product.name,
                        "combo_sf_product_id": combo_sf_product.sf_product_id,
                        "erpnext_item": combo_sf_product.item_link,
                        "combo_quantity": combo_item.quantity,
                        "order_quantity": order_quantity
                    }
                )
                return None
                
            item_quantity = combo_item.quantity * order_quantity
            expanded_items[combo_sf_product.item_link] = expanded_items.get(combo_sf_product.item_link, 0) + item_quantity
        
        debug_print(f"Combo product {sf_product.name} expanded to {len(expanded_items)} items")
        return expanded_items
        
    except Exception as e:
        error_print(f"Error validating combo product {sf_product.name} for order {order_name}: {str(e)}")
        log_error(
            error_category="System Error",
            error_description=f"Error validating combo product {sf_product.name}: {str(e)}",
            processing_stage="Item Validation",
            entity_type="Combo Product",
            external_id=sf_product.sf_product_id,
            reference_doctype="SF Product Master",
            internal_reference=sf_product.name,
            error_severity="Medium",
            additional_detail={
                "order_name": order_name,
                "sf_product_name": sf_product.name,
                "sf_product_id": sf_product.sf_product_id,
                "order_quantity": order_quantity,
                "error": str(e)
            }
        )
        return None


def update_error_log_categories():
    """
    Update the SF Inventory Data Import Error Logs with new categories for order aggregation
    This function can be run once to add new options to the select fields
    """
    try:
        # Get the current doctype
        error_log_doctype = frappe.get_doc("DocType", "SF Inventory Data Import Error Logs")
        
        # Update error category options
        current_error_categories = error_log_doctype.fields[6].options.split('\n')
        new_error_categories = [
            "Order Aggregation",
            "Sales Order Creation",
            "Item Processing",
            "Warehouse Hierarchy",
            "Customer Validation",
            "Order Status Update"
        ]
        
        # Add new categories if they don't exist
        updated_categories = current_error_categories.copy()
        for category in new_error_categories:
            if category not in current_error_categories:
                updated_categories.append(category)
        
        error_log_doctype.fields[6].options = '\n'.join(updated_categories)
        
        # Update processing stage options
        current_processing_stages = error_log_doctype.fields[8].options.split('\n')
        new_processing_stages = [
            "Order Aggregation",
            "Sales Order Creation",
            "Item Processing",
            "Warehouse Hierarchy",
            "Order Status Update"
        ]
        
        # Add new stages if they don't exist
        updated_stages = current_processing_stages.copy()
        for stage in new_processing_stages:
            if stage not in current_processing_stages:
                updated_stages.append(stage)
        
        error_log_doctype.fields[8].options = '\n'.join(updated_stages)
        
        # Save the doctype
        error_log_doctype.save()
        info_print("Successfully updated SF Inventory Data Import Error Logs with new categories")
        
    except Exception as e:
        error_print(f"Error updating error log categories: {str(e)}")
        log_error(
            error_category="System Error",
            error_description=f"Failed to update error log categories: {str(e)}",
            processing_stage="System Setup",
            entity_type="External Mapping",
            error_severity="Low",
            additional_detail={"error": str(e)}
        )


def create_combined_sales_orders(d2c_orders: Dict, b2b_orders: Dict, order_date: str) -> Dict:
    """
    Create combined sales orders for both D2C and B2B orders
    Combines D2C and B2B for internal transfers, separates for customer deliveries
    """
    info_print("Creating combined sales orders for D2C and B2B orders")
    
    # Remove transaction management - let individual functions handle their own transactions
    # frappe.db.begin()  # REMOVED - this was causing the implicit commit error
    
    try:
        created_orders = []
        
        # Get default company and internal customer
        default_company = frappe.defaults.get_defaults().get("company")
        internal_customer = get_internal_customer()
        
        debug_print(f"Using company: {default_company}, customer: {internal_customer}")
        
        # Combine D2C and B2B orders by plant and DC for internal transfers
        combined_orders = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"items": defaultdict(float), "orders": []})))
        
        # Add D2C items and orders
        for plant, plant_data in d2c_orders.items():
            for dc, dc_data in plant_data.items():
                for darkstore, darkstore_data in dc_data.items():
                    # Add items
                    for item_code, quantity in darkstore_data.get("items", {}).items():
                        combined_orders[plant][dc][darkstore]["items"][item_code] += quantity
                    # Add orders
                    combined_orders[plant][dc][darkstore]["orders"].extend(darkstore_data.get("orders", []))
        
        # Add B2B items and orders
        for plant, plant_data in b2b_orders.items():
            for dc, dc_data in plant_data.items():
                for darkstore, darkstore_data in dc_data.items():
                    # Add items
                    for item_code, quantity in darkstore_data.get("items", {}).items():
                        combined_orders[plant][dc][darkstore]["items"][item_code] += quantity
                    # Add orders
                    combined_orders[plant][dc][darkstore]["orders"].extend(darkstore_data.get("orders", []))
        
        # Now create sales orders
        for plant, plant_data in combined_orders.items():
            for dc, dc_data in plant_data.items():
                
                # Create ONE distribution center order (from plant to DC) with ALL items
                dc_order = create_sales_order_for_distribution_center(
                    plant, dc, dc_data, order_date, internal_customer, default_company, True, "Internal"
                )
                if dc_order:
                    created_orders.append(dc_order)
                    info_print(f"Created combined DC order: {dc_order['sales_order']}")
                
                # Process each darkstore
                for darkstore, darkstore_data in dc_data.items():
                    if darkstore == "DC_DIRECT":
                        # Direct DC orders - create customer-specific orders for B2B only
                        b2b_orders_list = [order for order in darkstore_data["orders"] if order.get("order_type") == "B2B"]
                        if b2b_orders_list:
                            customer_groups = group_b2b_orders_by_customer(b2b_orders_list)
                            for customer, customer_orders in customer_groups.items():
                                if customer != internal_customer:
                                    dc_direct_order = create_sales_order_for_dc_direct_with_orders(
                                        dc, customer_orders, order_date, customer, default_company, False
                                    )
                                    if dc_direct_order:
                                        created_orders.append(dc_direct_order)
                                        info_print(f"Created DC direct order: {dc_direct_order['sales_order']}")
                                        
                    elif darkstore == "CLIENT":
                        # Direct plant to client orders - create customer-specific orders for B2B only
                        b2b_orders_list = [order for order in darkstore_data["orders"] if order.get("order_type") == "B2B"]
                        if b2b_orders_list:
                            customer_groups = group_b2b_orders_by_customer(b2b_orders_list)
                            for customer, customer_orders in customer_groups.items():
                                if customer != internal_customer:
                                    plant_direct_order = create_sales_order_for_plant_direct_with_orders(
                                        plant, customer_orders, order_date, customer, default_company, False
                                    )
                                    if plant_direct_order:
                                        created_orders.append(plant_direct_order)
                                        info_print(f"Created plant direct order: {plant_direct_order['sales_order']}")
                    else:
                        # Regular darkstore orders
                        # Create ONE internal transfer (DC to Darkstore) with ALL items
                        if darkstore_data.get("items"):
                            darkstore_order = create_sales_order_for_darkstore(
                                dc, darkstore, darkstore_data, order_date, internal_customer, default_company, True, "Internal"
                            )
                            if darkstore_order:
                                created_orders.append(darkstore_order)
                                info_print(f"Created combined darkstore order: {darkstore_order['sales_order']}")
                        
                        # Create customer-specific orders from darkstore to client (B2B only)
                        b2b_orders_list = [order for order in darkstore_data["orders"] if order.get("order_type") == "B2B"]
                        if b2b_orders_list:
                            customer_groups = group_b2b_orders_by_customer(b2b_orders_list)
                            for customer, customer_orders in customer_groups.items():
                                if customer != internal_customer:
                                    darkstore_to_client_order = create_sales_order_for_darkstore_to_client_with_orders(
                                        darkstore, customer_orders, order_date, customer, default_company, False
                                    )
                                    if darkstore_to_client_order:
                                        created_orders.append(darkstore_to_client_order)
                                        info_print(f"Created darkstore to client order: {darkstore_to_client_order['sales_order']}")
        
        # Remove commit - let individual functions handle their own transactions
        # frappe.db.commit()  # REMOVED
        
        # Separate orders by type for reporting
        d2c_orders_created = [order for order in created_orders if order.get("order_type") in ["D2C", "Internal"]]
        b2b_orders_created = [order for order in created_orders if order.get("order_type") == "B2B"]
        
        return {
            "d2c_orders": d2c_orders_created,
            "b2b_orders": b2b_orders_created,
            "total": len(created_orders)
        }
        
    except Exception as e:
        # Remove rollback - no transaction was started at this level
        # frappe.db.rollback()  # REMOVED
        error_print(f"Error in create_combined_sales_orders: {str(e)}")
        log_error(
            error_category="Sales Order Creation",
            error_description=f"Failed to create combined sales orders: {str(e)}",
            processing_stage="Sales Order Creation",
            entity_type="Sales Order",
            error_severity="Critical",
            additional_detail={
                "order_date": order_date,
                "error": str(e)
            }
        )
        return {
            "d2c_orders": [],
            "b2b_orders": [],
            "total": 0
        }


def group_b2b_orders_by_customer(orders: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group B2B orders by customer
    Uses SF Inventory External ID Mapping to look up customer_id to ERPNext Customer
    """
    customer_groups = defaultdict(list)
    
    for order in orders:
        customer_id = order.get("customer_id")
        if customer_id:
            # Look up customer mapping
            customer_mapping = frappe.db.get_value(
                "SF Inventory External ID Mapping",
                {
                    "entity_type": "Customer",
                    "external_id": customer_id
                },
                ["internal_reference", "reference_doctype"],
                as_dict=True
            )
            
            if customer_mapping and customer_mapping.internal_reference and customer_mapping.reference_doctype == "Customer":
                if frappe.db.exists("Customer", customer_mapping.internal_reference):
                    customer_groups[customer_mapping.internal_reference].append(order)
                    debug_print(f"Grouped order {order.name} under customer {customer_mapping.internal_reference}")
                else:
                    debug_print(f"Customer {customer_mapping.internal_reference} does not exist, skipping order {order.name}")
            else:
                debug_print(f"No valid customer mapping for customer_id {customer_id}, skipping order {order.name}")
        else:
            debug_print(f"No customer_id in order {order.name}, skipping")
    
    return dict(customer_groups)


def create_sales_order_for_distribution_center_with_items(plant_warehouse: str, dc_warehouse: str,
                                                        aggregated_items: Dict[str, float], order_date: str,
                                                        customer: str, company: str, is_internal: bool, order_type: str) -> Optional[Dict]:
    """
    Create sales order for distribution center with specific items
    """
    try:
        debug_print(f"Creating sales order for distribution center {dc_warehouse} with {len(aggregated_items)} items")
        
        if not aggregated_items:
            debug_print(f"No items to aggregate for DC {dc_warehouse}")
            return None
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": plant_warehouse,  # Source warehouse (Plant)
            "items": []
        }
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = dc_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add aggregated items
        for item_code, quantity in aggregated_items.items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": plant_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to DC sales order for {dc_warehouse}")
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        debug_print(f"Successfully created DC sales order {sales_order.name}")
        
        return {
            "type": "distribution_center_order",
            "order_type": order_type,
            "sales_order": sales_order.name,
            "from_warehouse": plant_warehouse,
            "to_warehouse": dc_warehouse,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        error_print(f"Error creating distribution center sales order: {str(e)}")
        log_error(
            error_category="Order Processing",
            error_description=f"Failed to create distribution center sales order: {str(e)}",
            processing_stage="Sales Order Creation",
            entity_type="Sales Order",
            reference_doctype="Sales Order",
            error_severity="High",
            additional_detail={
                "from_warehouse": plant_warehouse,
                "to_warehouse": dc_warehouse,
                "order_type": order_type,
                "customer": customer,
                "error": str(e)
            }
        )
        return None


def create_sales_order_for_dc_direct_with_orders(dc_warehouse: str, customer_orders: List[Dict], order_date: str,
                                               customer: str, company: str, is_internal: bool) -> Optional[Dict]:
    """
    Create sales order for direct DC to client with specific orders
    """
    # Start transaction for this sales order creation
    frappe.db.begin()
    
    try:
        debug_print(f"Creating direct DC to client sales order for {dc_warehouse} with {len(customer_orders)} orders")
        
        # Aggregate items from the specific customer orders
        aggregated_items = defaultdict(float)
        for order in customer_orders:
            order_items = frappe.db.sql("""
                SELECT item_id, item_name, quantity, sf_product_master
                FROM `tabSF Order Item`
                WHERE parent = %(order_name)s
                AND (is_item_processed IS NULL OR is_item_processed = 0)
            """, {"order_name": order.name}, as_dict=True)
            
            for order_item in order_items:
                if order_item.sf_product_master:
                    sf_product = frappe.get_doc("SF Product Master", order_item.sf_product_master)
                    if sf_product.is_combo:
                        combo_items = validate_and_expand_combo_items(sf_product, order_item.quantity, order.name)
                        if combo_items:
                            for item_code, qty in combo_items.items():
                                aggregated_items[item_code] += qty
                    else:
                        if sf_product.item_link:
                            aggregated_items[sf_product.item_link] += order_item.quantity
        
        if not aggregated_items:
            debug_print(f"No items to aggregate for DC direct order {dc_warehouse}")
            frappe.db.rollback()
            return None
        
        # Get customer address for shipping
        customer_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": dc_warehouse,  # Source warehouse (DC)
            "items": []
        }
        
        if customer_address:
            sales_order_data["shipping_address_name"] = customer_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = dc_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add aggregated items
        for item_code, quantity in aggregated_items.items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": dc_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to DC direct sales order for {dc_warehouse}")
            frappe.db.rollback()
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        # Commit the transaction
        frappe.db.commit()
        
        debug_print(f"Successfully created DC direct sales order {sales_order.name}")
        
        return {
            "type": "dc_direct_order",
            "order_type": "B2B",
            "sales_order": sales_order.name,
            "from_warehouse": dc_warehouse,
            "to_customer": customer,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        # Rollback on error
        frappe.db.rollback()
        error_print(f"Error creating DC direct sales order: {str(e)}")
        return None


def create_sales_order_for_plant_direct_with_orders(plant_warehouse: str, customer_orders: List[Dict], order_date: str,
                                                  customer: str, company: str, is_internal: bool) -> Optional[Dict]:
    """
    Create sales order for direct plant to client with specific orders
    """
    # Start transaction for this sales order creation
    frappe.db.begin()
    
    try:
        debug_print(f"Creating direct plant to client sales order for {plant_warehouse} with {len(customer_orders)} orders")
        
        # Aggregate items from the specific customer orders
        aggregated_items = defaultdict(float)
        for order in customer_orders:
            order_items = frappe.db.sql("""
                SELECT item_id, item_name, quantity, sf_product_master
                FROM `tabSF Order Item`
                WHERE parent = %(order_name)s
                AND (is_item_processed IS NULL OR is_item_processed = 0)
            """, {"order_name": order.name}, as_dict=True)
            
            for order_item in order_items:
                if order_item.sf_product_master:
                    sf_product = frappe.get_doc("SF Product Master", order_item.sf_product_master)
                    if sf_product.is_combo:
                        combo_items = validate_and_expand_combo_items(sf_product, order_item.quantity, order.name)
                        if combo_items:
                            for item_code, qty in combo_items.items():
                                aggregated_items[item_code] += qty
                    else:
                        if sf_product.item_link:
                            aggregated_items[sf_product.item_link] += order_item.quantity
        
        if not aggregated_items:
            debug_print(f"No items to aggregate for plant direct order {plant_warehouse}")
            frappe.db.rollback()
            return None
        
        # Get customer address for shipping
        customer_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": plant_warehouse,  # Source warehouse (Plant)
            "items": []
        }
        
        if customer_address:
            sales_order_data["shipping_address_name"] = customer_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = plant_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add aggregated items
        for item_code, quantity in aggregated_items.items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": plant_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to plant direct sales order for {plant_warehouse}")
            frappe.db.rollback()
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        # Commit the transaction
        frappe.db.commit()
        
        debug_print(f"Successfully created plant direct sales order {sales_order.name}")
        
        return {
            "type": "plant_direct_order",
            "order_type": "B2B",
            "sales_order": sales_order.name,
            "from_warehouse": plant_warehouse,
            "to_customer": customer,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        # Rollback on error
        frappe.db.rollback()
        error_print(f"Error creating plant direct sales order: {str(e)}")
        return None


def create_sales_order_for_darkstore_to_client_with_orders(darkstore_warehouse: str, customer_orders: List[Dict], order_date: str,
                                                         customer: str, company: str, is_internal: bool) -> Optional[Dict]:
    """
    Create sales order for Darkstore to Client with specific orders
    """
    # Start transaction for this sales order creation
    frappe.db.begin()
    
    try:
        debug_print(f"Creating darkstore to client sales order for {darkstore_warehouse} with {len(customer_orders)} orders")
        
        # Aggregate items from the specific customer orders
        aggregated_items = defaultdict(float)
        for order in customer_orders:
            order_items = frappe.db.sql("""
                SELECT item_id, item_name, quantity, sf_product_master
                FROM `tabSF Order Item`
                WHERE parent = %(order_name)s
                AND (is_item_processed IS NULL OR is_item_processed = 0)
            """, {"order_name": order.name}, as_dict=True)
            
            for order_item in order_items:
                if order_item.sf_product_master:
                    sf_product = frappe.get_doc("SF Product Master", order_item.sf_product_master)
                    if sf_product.is_combo:
                        combo_items = validate_and_expand_combo_items(sf_product, order_item.quantity, order.name)
                        if combo_items:
                            for item_code, qty in combo_items.items():
                                aggregated_items[item_code] += qty
                    else:
                        if sf_product.item_link:
                            aggregated_items[sf_product.item_link] += order_item.quantity
        
        if not aggregated_items:
            debug_print(f"No items to aggregate for darkstore to client order {darkstore_warehouse}")
            frappe.db.rollback()
            return None
        
        # Get customer address for shipping
        customer_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": darkstore_warehouse,  # Source warehouse (Darkstore)
            "items": []
        }
        
        if customer_address:
            sales_order_data["shipping_address_name"] = customer_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = darkstore_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add aggregated items
        for item_code, quantity in aggregated_items.items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": darkstore_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to darkstore to client sales order for {darkstore_warehouse}")
            frappe.db.rollback()
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        # Commit the transaction
        frappe.db.commit()
        
        debug_print(f"Successfully created darkstore to client sales order {sales_order.name}")
        
        return {
            "type": "darkstore_to_client_order",
            "order_type": "B2B",
            "sales_order": sales_order.name,
            "from_warehouse": darkstore_warehouse,
            "to_customer": customer,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        # Rollback on error
        frappe.db.rollback()
        error_print(f"Error creating darkstore to client sales order: {str(e)}")
        log_error(
            error_category="Order Processing",
            error_description=f"Failed to create darkstore to client sales order: {str(e)}",
            processing_stage="Sales Order Creation",
            entity_type="Sales Order",
            reference_doctype="Sales Order",
            error_severity="High",
            additional_detail={
                "from_warehouse": darkstore_warehouse,
                "to_customer": customer,
                "order_type": "B2B",
                "customer": customer,
                "error": str(e)
            }
        )
        return None


def get_b2b_customer_from_orders(dc_data: Dict) -> str:
    """
    Get the customer from B2B orders in the DC data
    Uses SF Inventory External ID Mapping to look up customer_id to ERPNext Customer
    """
    for darkstore_data in dc_data.values():
        for order in darkstore_data.get("orders", []):
            # Get customer_id from the order
            customer_id = order.get("customer_id")
            if customer_id:
                # Look up in SF Inventory External ID Mapping
                customer_mapping = frappe.db.get_value(
                    "SF Inventory External ID Mapping",
                    {
                        "entity_type": "Customer",
                        "external_id": customer_id
                    },
                    ["internal_reference", "reference_doctype"],
                    as_dict=True
                )
                
                if customer_mapping and customer_mapping.internal_reference:
                    # Verify the reference_doctype is Customer
                    if customer_mapping.reference_doctype == "Customer":
                        # Verify the customer exists in ERPNext
                        if frappe.db.exists("Customer", customer_mapping.internal_reference):
                            debug_print(f"Found customer mapping: {customer_id} -> {customer_mapping.internal_reference}")
                            return customer_mapping.internal_reference
                        else:
                            error_print(f"Customer {customer_mapping.internal_reference} from mapping does not exist in ERPNext")
                            log_error(
                                error_category="Missing Reference",
                                error_description=f"Customer {customer_mapping.internal_reference} from mapping does not exist in ERPNext",
                                processing_stage="Customer Validation",
                                entity_type="Customer",
                                external_id=customer_id,
                                reference_doctype="Customer",
                                internal_reference=customer_mapping.internal_reference,
                                error_severity="High",
                                additional_detail={
                                    "order_name": order.name,
                                    "order_id": order.order_id,
                                    "customer_id": customer_id,
                                    "mapped_customer": customer_mapping.internal_reference
                                }
                            )
                    else:
                        error_print(f"Invalid reference_doctype in customer mapping: {customer_mapping.reference_doctype}")
                        log_error(
                            error_category="Data Mapping",
                            error_description=f"Invalid reference_doctype in customer mapping: {customer_mapping.reference_doctype}",
                            processing_stage="Customer Validation",
                            entity_type="Customer",
                            external_id=customer_id,
                            reference_doctype="SF Inventory External ID Mapping",
                            error_severity="High",
                            additional_detail={
                                "order_name": order.name,
                                "order_id": order.order_id,
                                "customer_id": customer_id,
                                "reference_doctype": customer_mapping.reference_doctype
                            }
                        )
                else:
                    error_print(f"No customer mapping found for customer_id: {customer_id}")
                    log_error(
                        error_category="Missing Reference",
                        error_description=f"No customer mapping found for customer_id: {customer_id}",
                        processing_stage="Customer Validation",
                        entity_type="Customer",
                        external_id=customer_id,
                        reference_doctype="SF Inventory External ID Mapping",
                        error_severity="High",
                        additional_detail={
                            "order_name": order.name,
                            "order_id": order.order_id,
                            "customer_id": customer_id
                        }
                    )
    
    # Fallback to internal customer if no valid customer found
    debug_print("No valid B2B customer found, falling back to internal customer")
    return get_internal_customer()


def create_sales_order_for_darkstore(dc_warehouse: str, darkstore_warehouse: str, 
                                   darkstore_data: Dict, order_date: str, 
                                   customer: str, company: str, is_internal: bool, order_type: str) -> Optional[Dict]:
    """
    Create sales order for darkstore (from distribution center to darkstore)
    """
    # Start transaction for this sales order creation
    frappe.db.begin()
    
    try:
        debug_print(f"Creating sales order for darkstore {darkstore_warehouse}")
        
        # Determine shipping address based on whether it's internal or external
        shipping_address = None
        if is_internal:
            # Internal transfers use destination warehouse (darkstore) address
            darkstore_facility = frappe.db.get_value(
                "SF Facility Master",
                {"warehouse": darkstore_warehouse, "type": "Darkstore"},
                ["shipping_address"],
                as_dict=True
            )
            
            if darkstore_facility and darkstore_facility.shipping_address:
                shipping_address = darkstore_facility.shipping_address
            else:
                # Fallback to warehouse address if facility address not found
                warehouse_address = frappe.db.get_value(
                    "Warehouse",
                    darkstore_warehouse,
                    ["address_line_1", "address_line_2", "city", "state", "pin"],
                    as_dict=True
                )
                if warehouse_address:
                    debug_print(f"Using warehouse address for darkstore {darkstore_warehouse}")
                else:
                    error_print(f"No shipping address found for darkstore {darkstore_warehouse}")
                    log_error(
                        error_category="Missing Reference",
                        error_description=f"No shipping address found for darkstore {darkstore_warehouse}",
                        processing_stage="Sales Order Creation",
                        entity_type="Facility",
                        external_id=darkstore_warehouse,
                        reference_doctype="SF Facility Master",
                        internal_reference=darkstore_warehouse,
                        error_severity="Medium",
                        additional_detail={
                            "warehouse": darkstore_warehouse,
                            "facility_type": "Darkstore",
                            "order_type": order_type
                        }
                    )
        else:
            # External orders use customer's shipping address
            shipping_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": dc_warehouse,  # Source warehouse (DC)
            "items": []
        }
        
        if shipping_address:
            sales_order_data["shipping_address_name"] = shipping_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = darkstore_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add items
        for item_code, quantity in darkstore_data["items"].items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": dc_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to darkstore sales order for {darkstore_warehouse}")
            frappe.db.rollback()
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        # Commit the transaction
        frappe.db.commit()
        
        debug_print(f"Successfully created darkstore sales order {sales_order.name}")
        
        return {
            "type": "darkstore_order",
            "order_type": order_type,
            "sales_order": sales_order.name,
            "from_warehouse": dc_warehouse,
            "to_warehouse": darkstore_warehouse,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        # Rollback on error
        frappe.db.rollback()
        error_print(f"Error creating darkstore sales order: {str(e)}")
        log_error(
            error_category="Order Processing",
            error_description=f"Failed to create darkstore sales order: {str(e)}",
            processing_stage="Sales Order Creation",
            entity_type="Sales Order",
            reference_doctype="Sales Order",
            error_severity="High",
            additional_detail={
                "from_warehouse": dc_warehouse,
                "to_warehouse": darkstore_warehouse,
                "order_type": order_type,
                "customer": customer,
                "error": str(e)
            }
        )
        return None


def create_sales_order_for_distribution_center(plant_warehouse: str, dc_warehouse: str,
                                             dc_data: Dict, order_date: str,
                                             customer: str, company: str, is_internal: bool, order_type: str) -> Optional[Dict]:
    """
    Create sales order for distribution center (from plant to distribution center)
    """
    # Start transaction for this sales order creation
    frappe.db.begin()
    
    try:
        debug_print(f"Creating sales order for distribution center {dc_warehouse}")
        
        # Aggregate all items from all darkstores under this DC
        aggregated_items = defaultdict(float)
        for darkstore_data in dc_data.values():
            if isinstance(darkstore_data, dict) and "items" in darkstore_data:
                for item_code, quantity in darkstore_data["items"].items():
                    aggregated_items[item_code] += quantity
        
        if not aggregated_items:
            debug_print(f"No items to aggregate for DC {dc_warehouse}")
            frappe.db.rollback()
            return None
        
        # Determine shipping address based on whether it's internal or external
        shipping_address = None
        if is_internal:
            # Internal transfers use destination warehouse (DC) address
            # First try to get address from SF Facility Master
            dc_facility = frappe.db.get_value(
                "SF Facility Master",
                {"warehouse": dc_warehouse, "type": "Distribution Center"},
                ["shipping_address"],
                as_dict=True
            )
            
            if dc_facility and dc_facility.shipping_address:
                shipping_address = dc_facility.shipping_address
            else:
                # Fallback to warehouse address
                warehouse_address = frappe.db.get_value(
                    "Warehouse",
                    dc_warehouse,
                    ["address_line_1", "address_line_2", "city", "state", "pin"],
                    as_dict=True
                )
                if warehouse_address:
                    debug_print(f"Using warehouse address for DC {dc_warehouse}")
        else:
            # External orders use customer's shipping address
            shipping_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": plant_warehouse,  # Source warehouse (Plant)
            "items": []
        }
        
        if shipping_address:
            sales_order_data["shipping_address_name"] = shipping_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = dc_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add aggregated items
        for item_code, quantity in aggregated_items.items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": plant_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to DC sales order for {dc_warehouse}")
            frappe.db.rollback()
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        # Commit the transaction
        frappe.db.commit()
        
        debug_print(f"Successfully created DC sales order {sales_order.name}")
        
        return {
            "type": "distribution_center_order",
            "order_type": order_type,
            "sales_order": sales_order.name,
            "from_warehouse": plant_warehouse,
            "to_warehouse": dc_warehouse,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        # Rollback on error
        frappe.db.rollback()
        error_print(f"Error creating distribution center sales order: {str(e)}")
        log_error(
            error_category="Order Processing",
            error_description=f"Failed to create distribution center sales order: {str(e)}",
            processing_stage="Sales Order Creation",
            entity_type="Sales Order",
            reference_doctype="Sales Order",
            error_severity="High",
            additional_detail={
                "from_warehouse": plant_warehouse,
                "to_warehouse": dc_warehouse,
                "order_type": order_type,
                "customer": customer,
                "error": str(e)
            }
        )
        return None


def create_sales_order_for_dc_direct(dc_warehouse: str, dc_data: Dict, order_date: str,
                                   customer: str, company: str, is_internal: bool) -> Optional[Dict]:
    """
    Create sales order for direct DC to client (B2B orders)
    """
    try:
        debug_print(f"Creating direct DC to client sales order for {dc_warehouse}")
        
        # Get customer address for shipping
        customer_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": dc_warehouse,  # Source warehouse (DC)
            "items": []
        }
        
        if customer_address:
            sales_order_data["shipping_address_name"] = customer_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = dc_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add items
        for item_code, quantity in dc_data["items"].items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": dc_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to DC direct sales order for {dc_warehouse}")
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        debug_print(f"Successfully created DC direct sales order {sales_order.name}")
        
        return {
            "type": "dc_direct_order",
            "order_type": "B2B",
            "sales_order": sales_order.name,
            "from_warehouse": dc_warehouse,
            "to_customer": customer,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        error_print(f"Error creating DC direct sales order: {str(e)}")
        return None


def create_sales_order_for_plant_direct(plant_warehouse: str, plant_data: Dict, order_date: str,
                                      customer: str, company: str, is_internal: bool) -> Optional[Dict]:
    """
    Create sales order for direct plant to client (B2B orders)
    """
    try:
        debug_print(f"Creating direct plant to client sales order for {plant_warehouse}")
        
        # Get customer address for shipping
        customer_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": plant_warehouse,  # Source warehouse (Plant)
            "items": []
        }
        
        if customer_address:
            sales_order_data["shipping_address_name"] = customer_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = plant_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add items
        for item_code, quantity in plant_data["items"].items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": plant_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to plant direct sales order for {plant_warehouse}")
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        debug_print(f"Successfully created plant direct sales order {sales_order.name}")
        
        return {
            "type": "plant_direct_order",
            "order_type": "B2B",
            "sales_order": sales_order.name,
            "from_warehouse": plant_warehouse,
            "to_customer": customer,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        error_print(f"Error creating plant direct sales order: {str(e)}")
        return None


def create_sales_order_for_darkstore_to_client(darkstore_warehouse: str, darkstore_data: Dict, order_date: str,
                                               customer: str, company: str, is_internal: bool) -> Optional[Dict]:
    """
    Create sales order for Darkstore to Client (B2B orders)
    This creates the final sales order from Darkstore directly to the B2B client
    """
    try:
        debug_print(f"Creating darkstore to client sales order for {darkstore_warehouse}")
        
        # Get customer address for shipping (B2B orders use customer address, not darkstore address)
        customer_address = get_customer_shipping_address(customer)
        
        # Create sales order
        sales_order_data = {
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": darkstore_warehouse,  # Source warehouse (Darkstore)
            "items": []
        }
        
        if customer_address:
            sales_order_data["shipping_address_name"] = customer_address
        
        # Only set target warehouse for internal customers
        if is_internal:
            sales_order_data["custom_set_target_warehouse"] = darkstore_warehouse
        
        sales_order = frappe.get_doc(sales_order_data)
        
        # Add items
        for item_code, quantity in darkstore_data["items"].items():
            if quantity > 0:
                sales_order.append("items", {
                    "item_code": item_code,
                    "qty": quantity,
                    "warehouse": darkstore_warehouse,
                    "delivery_date": add_days(order_date, 1)
                })
        
        if not sales_order.items:
            debug_print(f"No items to add to darkstore to client sales order for {darkstore_warehouse}")
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        debug_print(f"Successfully created darkstore to client sales order {sales_order.name}")
        
        return {
            "type": "darkstore_to_client_order",
            "order_type": "B2B",
            "sales_order": sales_order.name,
            "from_warehouse": darkstore_warehouse,
            "to_customer": customer,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        error_print(f"Error creating darkstore to client sales order: {str(e)}")
        log_error(
            error_category="Order Processing",
            error_description=f"Failed to create darkstore to client sales order: {str(e)}",
            processing_stage="Sales Order Creation",
            entity_type="Sales Order",
            reference_doctype="Sales Order",
            error_severity="High",
            additional_detail={
                "from_warehouse": darkstore_warehouse,
                "to_customer": customer,
                "order_type": "B2B",
                "customer": customer,
                "error": str(e)
            }
        )
        return None


def get_customer_shipping_address(customer: str) -> Optional[str]:
    """
    Get the shipping address for a customer
    """
    try:
        customer_doc = frappe.get_doc("Customer", customer)
        if customer_doc.customer_primary_address:
            return customer_doc.customer_primary_address
        return None
    except:
        return None


def mark_orders_and_items_as_processed(d2c_processing_results: Dict, b2b_processing_results: Dict) -> None:
    """
    Mark successfully processed orders and items as processed
    
    Args:
        d2c_processing_results: D2C order processing results
        b2b_processing_results: B2B order processing results
    """
    debug_print("Marking orders and items as processed")
    
    # Start transaction for this function
    frappe.db.begin()
    
    try:
        # Process D2C orders
        mark_processing_results(d2c_processing_results, "D2C")
        
        # Process B2B orders
        mark_processing_results(b2b_processing_results, "B2B")
        
        # Commit the changes
        frappe.db.commit()
        info_print("Successfully marked orders and items as processed")
        
    except Exception as e:
        # Rollback on error
        frappe.db.rollback()
        error_print(f"Error in mark_orders_and_items_as_processed: {str(e)}")
        # Log the error
        log_error(
            error_category="Order Processing",
            error_description=f"Failed to mark orders and items as processed: {str(e)}",
            processing_stage="Post Processing",
            entity_type="Order Processing",
            error_severity="Medium",
            additional_detail={
                "error": str(e)
            }
        )


def mark_processing_results(processing_results: Dict, order_type: str) -> None:
    """
    Mark processing results for a specific order type
    """
    for order_name, item_details in processing_results["item_processing_details"].items():
        try:
            # Count successful and total items
            successful_items = [details for details in item_details.values() if details["success"]]
            total_items = len(item_details)
            
            # Mark individual items as processed
            for item_id, details in item_details.items():
                if details["success"]:
                    # Mark item as processed
                    frappe.db.set_value("SF Order Item", details["item_row_name"], "is_item_processed", 1)
                    debug_print(f"Marked item {item_id} in order {order_name} as processed")
            
            # Determine order status
            if len(successful_items) == 0:
                order_status = "Unprocessed"
            elif len(successful_items) == total_items:
                order_status = "Processed"
            else:
                order_status = "Partially Processed"
            
            # Mark order status
            frappe.db.set_value("SF Order Master", order_name, "processing_status", order_status)
            debug_print(f"Marked order {order_name} as {order_status} ({len(successful_items)}/{total_items} items)")
            
        except Exception as e:
            error_print(f"Error marking order {order_name} as processed: {str(e)}")
            log_error(
                error_category="Order Processing",
                error_description=f"Failed to mark order {order_name} as processed: {str(e)}",
                processing_stage="Post Processing",
                entity_type=f"Order {order_type}",
                reference_doctype="SF Order Master",
                internal_reference=order_name,
                error_severity="Medium",
                additional_detail={
                    "order_name": order_name,
                    "order_type": order_type,
                    "error": str(e)
                }
            )


def get_internal_customer():
    """
    Get internal customer for default company
    """
    default_company = frappe.defaults.get_defaults().get("company")
    internal_customer = frappe.get_value("Customer", {"is_internal_customer": 1, "represents_company": default_company}, "name")
    
    if not internal_customer:
        frappe.throw(_("No internal customer found for company {0}").format(default_company))
    
    return internal_customer


# Utility function for testing
@frappe.whitelist()
def test_aggregate_orders(branches_json: str, order_date: str) -> Dict[str, Any]:
    """
    Test function that can be called from the UI
    
    Args:
        branches_json: JSON string of branch names
        order_date: Date in YYYY-MM-DD format
    
    Returns:
        Result dictionary
    """
    try:
        branches = json.loads(branches_json) if isinstance(branches_json, str) else branches_json
        info_print(f"Test run started - branches: {branches}, date: {order_date}")
        result = aggregate_orders_and_create_sales_orders(branches, order_date)
        info_print(f"Test run completed - status: {result['status']}")
        return result
    except Exception as e:
        error_print(f"Test run failed: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


# Scheduled job function
def daily_order_aggregation():
    """
    Daily scheduled job to aggregate orders
    This can be configured in hooks.py for automatic execution
    """
    try:
        info_print("Starting daily order aggregation job")
        
        # Get all active branches
        branches = frappe.db.sql("""
            SELECT name
            FROM `tabBranch`
        """, as_dict=True)
        
        branch_names = [branch.name for branch in branches]
        # yesterday = add_days(nowdate(), -1) # we do not need to add -1 as we are using today's date
        current_date = today()

        # branch_names = ["Hyderabad"]
        # yesterday = "2025-08-10"
        
        info_print(f"Processing branches: {branch_names}, date: {current_date}")
        
        result = aggregate_orders_and_create_sales_orders(branch_names, current_date)
        
        if result["status"] == "success":
            info_print(f"Daily order aggregation completed: {result['message']}")
            return result
        else:
            error_print(f"Daily order aggregation failed: {result['message']}")
            # Log the error with more details
            log_error(
                error_category="System Error",
                error_description=f"Daily order aggregation failed: {result['message']}",
                processing_stage="Order Aggregation",
                entity_type="Order D2C",
                error_severity="Critical",
                additional_detail={
                    "branches": branch_names,
                    "order_date": current_date,
                    "error_message": result['message'],
                    "function": "daily_order_aggregation"
                }
            )
            return result
            
    except Exception as e:
        error_print(f"Error in daily_order_aggregation: {str(e)}")
        # Log the error with more details
        log_error(
            error_category="System Error",
            error_description=f"Daily order aggregation exception: {str(e)}",
            processing_stage="Order Aggregation",
            entity_type="Order D2C",
            error_severity="Critical",
            additional_detail={
                "branches": branch_names if 'branch_names' in locals() else [],
                "order_date": current_date if 'current_date' in locals() else None,
                "error": str(e),
                "function": "daily_order_aggregation",
                "exception_type": type(e).__name__
            }
        )
        return {
            "status": "error",
            "message": f"Error in daily_order_aggregation: {str(e)}",
            "error_details": {
                "exception_type": type(e).__name__,
                "error_message": str(e),
                "function": "daily_order_aggregation"
            }
        }


# bench execute "inv_mgmt.cron_functions.aggregate_order_data.daily_order_aggregation"
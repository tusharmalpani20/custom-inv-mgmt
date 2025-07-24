import frappe
from frappe import _
from frappe.utils import getdate, nowdate, add_days
import json
from typing import List, Dict, Any, Optional
from collections import defaultdict


def aggregate_orders_and_create_sales_orders(branches: List[str], order_date: str) -> Dict[str, Any]:
    """
    Main function to aggregate orders from SF Order Master for specific branches and date,
    then create cyclic sales orders based on plant -> distribution center -> darkstore hierarchy.
    
    Args:
        branches: List of branch names to process
        order_date: Date to fetch orders for (YYYY-MM-DD format)
    
    Returns:
        Dict with aggregation results and created sales orders
    """
    try:
        # Step 1: Build warehouse hierarchy
        hierarchy = build_warehouse_hierarchy(branches)
        
        # Step 2: Get D2C orders for the specified date and plants
        orders = get_d2c_orders_for_date_and_plants(order_date, list(hierarchy.keys()))
        
        # Step 3: Group orders by hierarchy (darkstore -> distribution center -> plant)
        grouped_orders = group_orders_by_hierarchy(orders, hierarchy)
        
        # Step 4: Process items and expand combos (validate items and discard invalid orders)
        processed_orders, discarded_orders = process_order_items(grouped_orders)
        
        # Step 5: Create cyclic sales orders
        created_sales_orders = create_cyclic_sales_orders(processed_orders, order_date)
        
        # Step 6: Mark successfully processed orders as processed
        processed_order_names = mark_orders_as_processed(processed_orders)
        
        return {
            "status": "success",
            "hierarchy": hierarchy,
            "total_orders_fetched": len(orders),
            "total_orders_processed": len(processed_order_names),
            "total_orders_discarded": len(discarded_orders),
            "discarded_orders": discarded_orders,
            "grouped_orders_count": len(processed_orders),
            "created_sales_orders": created_sales_orders,
            "processed_order_names": processed_order_names,
            "message": f"Successfully processed {len(processed_order_names)} orders, discarded {len(discarded_orders)} orders, and created {len(created_sales_orders)} sales orders"
        }
        
    except Exception as e:
        frappe.log_error(f"Error in aggregate_orders_and_create_sales_orders: {str(e)}")
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
    hierarchy = {}
    
    # Get all plants for the specified branches
    plants = frappe.db.sql("""
        SELECT name, warehouse_name, custom_branch
        FROM `tabWarehouse`
        WHERE custom_warehouse_category = 'Plant'
        AND custom_branch IN %(branches)s
        AND disabled = 0
    """, {"branches": branches}, as_dict=True)
    
    for plant in plants:
        plant_facility = frappe.db.get_value(
            "SF Facility Master",
            {"warehouse": plant.name, "type": "Plant"},
            ["name", "facility_id", "facility_name", "shipping_address"],
            as_dict=True
        )
        
        if not plant_facility:
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
    
    return hierarchy


def get_d2c_orders_for_date_and_plants(order_date: str, plant_names: List[str]) -> List[Dict]:
    """
    Get D2C orders for specific date and plants that haven't been processed yet
    
    Args:
        order_date: Date in YYYY-MM-DD format
        plant_names: List of plant warehouse names
    
    Returns:
        List of order dictionaries
    """
    # Get SF Facility Master names for the plants
    plant_facilities = frappe.db.sql("""
        SELECT name
        FROM `tabSF Facility Master`
        WHERE warehouse IN %(plants)s
        AND type = 'Plant'
    """, {"plants": plant_names}, as_dict=True)
    
    if not plant_facilities:
        return []
    
    plant_facility_names = [pf.name for pf in plant_facilities]
    
    # Get D2C orders for the date and plants that haven't been processed
    orders = frappe.db.sql("""
        SELECT *
        FROM `tabSF Order Master`
        WHERE order_type = 'D2C'
        AND order_date = %(order_date)s
        AND plant IN %(plant_facilities)s
        AND (is_order_processed IS NULL OR is_order_processed = 0)
        ORDER BY plant, darkstore
    """, {
        "order_date": order_date,
        "plant_facilities": plant_facility_names
    }, as_dict=True)
    
    return orders


def group_orders_by_hierarchy(orders: List[Dict], hierarchy: Dict) -> Dict:
    """
    Group orders by darkstore -> distribution center -> plant hierarchy
    
    Args:
        orders: List of SF Order Master records
        hierarchy: Warehouse hierarchy dict
    
    Returns:
        Grouped orders by hierarchy
    """
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
    
    for order in orders:
        plant_facility = order.get("plant")
        darkstore_facility = order.get("darkstore")
        
        if not plant_facility or not darkstore_facility:
            continue
            
        # Find the corresponding warehouses
        plant_info = facility_to_warehouse.get(plant_facility)
        darkstore_info = facility_to_warehouse.get(darkstore_facility)
        
        if not plant_info or not darkstore_info:
            continue
            
        plant_warehouse = plant_info["warehouse"]
        darkstore_warehouse = darkstore_info["warehouse"]
        dc_warehouse = darkstore_info["dc"]
        
        grouped[plant_warehouse][dc_warehouse][darkstore_warehouse].append(order)
    
    return dict(grouped)


def process_order_items(grouped_orders: Dict) -> tuple[Dict, List[Dict]]:
    """
    Process order items and expand combo items to individual items.
    Discard entire orders if any item doesn't have a valid ERPNext item link.
    
    Args:
        grouped_orders: Orders grouped by hierarchy
    
    Returns:
        Tuple of (processed_orders, discarded_orders)
    """
    processed = {}
    discarded_orders = []
    
    for plant, plant_data in grouped_orders.items():
        processed[plant] = {}
        for dc, dc_data in plant_data.items():
            processed[plant][dc] = {}
            for darkstore, orders in dc_data.items():
                processed[plant][dc][darkstore] = {
                    "orders": [],
                    "items": defaultdict(float)  # item_code -> total_quantity
                }
                
                # Process each order's items
                for order in orders:
                    order_valid = True
                    order_items_processed = {}
                    
                    order_items = frappe.db.sql("""
                        SELECT item_id, item_name, quantity, sf_product_master
                        FROM `tabSF Order Item`
                        WHERE parent = %(order_name)s
                    """, {"order_name": order.name}, as_dict=True)
                    
                    # First pass: validate all items in the order
                    for order_item in order_items:
                        if not order_item.sf_product_master:
                            frappe.log_error(f"Order {order.name}: SF Product Master not found for item {order_item.item_id}")
                            order_valid = False
                            break
                            
                        try:
                            sf_product = frappe.get_doc("SF Product Master", order_item.sf_product_master)
                        except frappe.DoesNotExistError:
                            frappe.log_error(f"Order {order.name}: SF Product Master {order_item.sf_product_master} does not exist")
                            order_valid = False
                            break
                        
                        if sf_product.is_combo:
                            # Validate all combo items have valid item links
                            combo_items = validate_and_expand_combo_items(sf_product, order_item.quantity, order.name)
                            if combo_items is None:  # Validation failed
                                order_valid = False
                                break
                            order_items_processed[order_item.item_id] = combo_items
                        else:
                            # Validate single item has valid item link
                            if not sf_product.item_link:
                                frappe.log_error(f"Order {order.name}: SF Product {sf_product.name} has no item_link to ERPNext Item")
                                order_valid = False
                                break
                            # Check if the linked item exists in ERPNext
                            if not frappe.db.exists("Item", sf_product.item_link):
                                frappe.log_error(f"Order {order.name}: ERPNext Item {sf_product.item_link} does not exist")
                                order_valid = False
                                break
                            order_items_processed[order_item.item_id] = {sf_product.item_link: order_item.quantity}
                    
                    # If order is valid, add it to processed orders
                    if order_valid:
                        processed[plant][dc][darkstore]["orders"].append(order)
                        
                        # Add items to the aggregated list
                        for item_dict in order_items_processed.values():
                            for item_code, qty in item_dict.items():
                                processed[plant][dc][darkstore]["items"][item_code] += qty
                    else:
                        # Add to discarded orders with reason
                        discarded_orders.append({
                            "order_name": order.name,
                            "order_id": order.order_id,
                            "plant": order.plant,
                            "darkstore": order.darkstore,
                            "reason": "One or more items have invalid or missing ERPNext item links"
                        })
                
                # Remove empty darkstore entries
                if not processed[plant][dc][darkstore]["orders"]:
                    del processed[plant][dc][darkstore]
            
            # Remove empty DC entries
            if not processed[plant][dc]:
                del processed[plant][dc]
        
        # Remove empty plant entries
        if not processed[plant]:
            del processed[plant]
    
    return processed, discarded_orders


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
    expanded_items = {}
    
    if not sf_product.combo_items:
        frappe.log_error(f"Order {order_name}: Combo product {sf_product.name} has no combo items defined")
        return None
    
    for combo_item in sf_product.combo_items:
        try:
            combo_sf_product = frappe.get_doc("SF Product Master", combo_item.sf_product_id)
        except frappe.DoesNotExistError:
            frappe.log_error(f"Order {order_name}: Combo item SF Product {combo_item.sf_product_id} does not exist")
            return None
            
        if not combo_sf_product.item_link:
            frappe.log_error(f"Order {order_name}: Combo item SF Product {combo_sf_product.name} has no item_link to ERPNext Item")
            return None
            
        # Check if the linked item exists in ERPNext
        if not frappe.db.exists("Item", combo_sf_product.item_link):
            frappe.log_error(f"Order {order_name}: ERPNext Item {combo_sf_product.item_link} does not exist")
            return None
            
        item_quantity = combo_item.quantity * order_quantity
        expanded_items[combo_sf_product.item_link] = expanded_items.get(combo_sf_product.item_link, 0) + item_quantity
    
    return expanded_items


def create_cyclic_sales_orders(processed_orders: Dict, order_date: str) -> List[Dict]:
    """
    Create cyclic sales orders: darkstore orders and distribution center orders
    
    Args:
        processed_orders: Processed orders with items
        order_date: Original order date
    
    Returns:
        List of created sales order details
    """
    created_orders = []
    
    # Get default company and internal customer
    default_company = frappe.defaults.get_defaults().get("company")
    internal_customer = get_or_create_internal_customer(default_company)
    
    for plant, plant_data in processed_orders.items():
        for dc, dc_data in plant_data.items():
            
            # Create distribution center order (from plant to DC)
            dc_order = create_sales_order_for_distribution_center(
                plant, dc, dc_data, order_date, internal_customer, default_company
            )
            if dc_order:
                created_orders.append(dc_order)
            
            for darkstore, darkstore_data in dc_data.items():
                # Create darkstore order (from DC to darkstore)
                darkstore_order = create_sales_order_for_darkstore(
                    dc, darkstore, darkstore_data, order_date, internal_customer, default_company
                )
                if darkstore_order:
                    created_orders.append(darkstore_order)
    
    return created_orders


def mark_orders_as_processed(processed_orders: Dict) -> List[str]:
    """
    Mark all successfully processed orders as processed in SF Order Master
    
    Args:
        processed_orders: Dictionary of processed orders
    
    Returns:
        List of order names that were marked as processed
    """
    processed_order_names = []
    
    try:
        for plant, plant_data in processed_orders.items():
            for dc, dc_data in plant_data.items():
                for darkstore, darkstore_data in dc_data.items():
                    for order in darkstore_data["orders"]:
                        try:
                            # Mark order as processed
                            frappe.db.set_value("SF Order Master", order.name, "is_order_processed", 1)
                            processed_order_names.append(order.name)
                        except Exception as e:
                            frappe.log_error(f"Error marking order {order.name} as processed: {str(e)}")
        
        # Commit the changes
        frappe.db.commit()
        
    except Exception as e:
        frappe.log_error(f"Error in mark_orders_as_processed: {str(e)}")
    
    return processed_order_names


def create_sales_order_for_darkstore(dc_warehouse: str, darkstore_warehouse: str, 
                                   darkstore_data: Dict, order_date: str, 
                                   customer: str, company: str) -> Optional[Dict]:
    """
    Create sales order for darkstore (from distribution center to darkstore)
    """
    try:
        # Get darkstore facility for shipping address
        darkstore_facility = frappe.db.get_value(
            "SF Facility Master",
            {"warehouse": darkstore_warehouse, "type": "Darkstore"},
            ["shipping_address"],
            as_dict=True
        )
        
        if not darkstore_facility or not darkstore_facility.shipping_address:
            frappe.log_error(f"No shipping address found for darkstore {darkstore_warehouse}")
            return None
        
        # Create sales order
        sales_order = frappe.get_doc({
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": dc_warehouse,  # Source warehouse (DC)
            "custom_set_target_warehouse": darkstore_warehouse,  # Target warehouse (Darkstore)
            "shipping_address_name": darkstore_facility.shipping_address,
            "items": []
        })
        
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
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        return {
            "type": "darkstore_order",
            "sales_order": sales_order.name,
            "from_warehouse": dc_warehouse,
            "to_warehouse": darkstore_warehouse,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        frappe.log_error(f"Error creating darkstore sales order: {str(e)}")
        return None


def create_sales_order_for_distribution_center(plant_warehouse: str, dc_warehouse: str,
                                             dc_data: Dict, order_date: str,
                                             customer: str, company: str) -> Optional[Dict]:
    """
    Create sales order for distribution center (from plant to distribution center)
    """
    try:
        # Aggregate all items from all darkstores under this DC
        aggregated_items = defaultdict(float)
        for darkstore_data in dc_data.values():
            if isinstance(darkstore_data, dict) and "items" in darkstore_data:
                for item_code, quantity in darkstore_data["items"].items():
                    aggregated_items[item_code] += quantity
        
        if not aggregated_items:
            return None
        
        # Get DC warehouse address for shipping
        dc_address = frappe.db.get_value(
            "Warehouse",
            dc_warehouse,
            ["address_line_1", "address_line_2", "city", "state", "pin"],
            as_dict=True
        )
        
        # Create sales order
        sales_order = frappe.get_doc({
            "doctype": "Sales Order",
            "customer": customer,
            "transaction_date": order_date,
            "delivery_date": add_days(order_date, 1),
            "company": company,
            "set_warehouse": plant_warehouse,  # Source warehouse (Plant)
            "custom_set_target_warehouse": dc_warehouse,  # Target warehouse (DC)
            "items": []
        })
        
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
            return None
            
        sales_order.insert()
        sales_order.submit()
        
        return {
            "type": "distribution_center_order",
            "sales_order": sales_order.name,
            "from_warehouse": plant_warehouse,
            "to_warehouse": dc_warehouse,
            "items_count": len(sales_order.items),
            "total_qty": sum(item.qty for item in sales_order.items)
        }
        
    except Exception as e:
        frappe.log_error(f"Error creating distribution center sales order: {str(e)}")
        return None


def get_or_create_internal_customer(company: str) -> str:
    """
    Get or create internal customer for the company
    
    Args:
        company: Company name
    
    Returns:
        Customer name
    """
    # Check if internal customer exists
    customer_name = f"{company} - Internal"
    
    if not frappe.db.exists("Customer", customer_name):
        # Create internal customer
        customer = frappe.get_doc({
            "doctype": "Customer",
            "customer_name": customer_name,
            "customer_type": "Company",
            "is_internal_customer": 1,
            "represents_company": company,
            "customer_group": frappe.db.get_single_value("Selling Settings", "customer_group") or "All Customer Groups",
            "territory": frappe.db.get_single_value("Selling Settings", "territory") or "All Territories"
        })
        customer.insert(ignore_permissions=True)
        return customer.name
    
    return customer_name


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
        return aggregate_orders_and_create_sales_orders(branches, order_date)
    except Exception as e:
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
        # # Get all active branches
        # branches = frappe.db.sql("""
        #     SELECT name
        #     FROM `tabBranch`
        #     WHERE disabled = 0
        # """, as_dict=True)
        
        # branch_names = [branch.name for branch in branches]
        # yesterday = add_days(nowdate(), -1)

        branch_names = ["Hyderabad"]
        yesterday = "2025-07-15"
        
        result = aggregate_orders_and_create_sales_orders(branch_names, yesterday)
        
        if result["status"] == "success":
            frappe.logger().info(f"Daily order aggregation completed: {result['message']}")
        else:
            frappe.log_error(f"Daily order aggregation failed: {result['message']}")
            
    except Exception as e:
        frappe.log_error(f"Error in daily_order_aggregation: {str(e)}")

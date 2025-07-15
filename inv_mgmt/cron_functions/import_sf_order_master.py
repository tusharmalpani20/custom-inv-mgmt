import frappe
import requests
from typing import Dict, Any, List
import json
from frappe.utils import now_datetime, get_datetime_str, today
from datetime import datetime
import time

# Time delay for D2C order API calls (5 seconds)
D2C_ORDER_API_DELAY_SECONDS = 5

def import_d2c_orders():
    """
    Import D2C orders from SF API and create SF Order Master records.
    Makes two API calls with 5 seconds delay as required.
    """
    try:
        print("Starting D2C order import process...")
        
        # Get API configuration from site config
        api_url = frappe.conf.get('sf_d2c_order_api_url')
        api_key = frappe.conf.get('sf_d2c_order_api_key')
        
        if not api_url or not api_key:
            frappe.throw("D2C order API configuration (sf_d2c_order_api_url, sf_d2c_order_api_key) missing in site config")
        
        # Prepare request headers and data
        headers = {
            'Authorization': f'{api_key}',
            'Content-Type': 'application/json'
        }
        
        request_data = {
            "delivery_date": today(),
            "regenerate": False
        }
        
        print(f"Making first API call to {api_url}")
        print(f"Request data: {json.dumps(request_data)}")
        
        # First API call
        first_response = requests.get(api_url, headers=headers, json=request_data)
        first_response.raise_for_status()
        
        print(f"First API call response: {first_response.json()}")
        
        # Wait for 5 seconds as specified
        print(f"Waiting {D2C_ORDER_API_DELAY_SECONDS} seconds before second API call...")
        time.sleep(D2C_ORDER_API_DELAY_SECONDS)
        
        # Second API call
        print("Making second API call...")
        second_response = requests.get(api_url, headers=headers, json=request_data)
        second_response.raise_for_status()
        
        response_data = second_response.json()
        print(f"Second API call response: {response_data}")
        
        if not response_data.get("success"):
            frappe.throw(f"API returned error: {response_data.get('message', 'Unknown error')}")
        
        orders_link = response_data.get("orders_link")
        if not orders_link:
            frappe.throw("No orders_link found in API response")
        
        print(f"Fetching orders data from: {orders_link}")
        
        # Fetch orders data from S3 link
        orders_response = requests.get(orders_link)
        orders_response.raise_for_status()
        orders_data = orders_response.json()
        
        if not isinstance(orders_data, list):
            frappe.throw("Expected orders data to be a list")
        
        print(f"Found {len(orders_data)} orders to process")
        
        # Start transaction for all order creation
        frappe.db.begin()
        
        success_count = 0
        error_count = 0
        errors = []
        
        for order_data in orders_data:
            try:
                print(f"Processing order: {order_data.get('order_id')}")
                create_order_master_record(order_data)
                success_count += 1
                print(f"Successfully created order: {order_data.get('order_id')}")
                
            except Exception as e:
                error_count += 1
                error_detail = {
                    "order_id": order_data.get("order_id"),
                    "error": str(e)
                }
                errors.append(error_detail)
                
                print(f"Error processing order {order_data.get('order_id')}: {str(e)}")
                frappe.log_error(
                    title=f"D2C Order Import Error - {order_data.get('order_id')}",
                    message=f"Error: {str(e)}\nOrder Data: {json.dumps(order_data, indent=2)}"
                )
        
        # Commit transaction
        frappe.db.commit()
        
        print(f"D2C order import completed. Success: {success_count}, Errors: {error_count}")
        
        return {
            "success": True,
            "message": f"D2C order import completed. Success: {success_count}, Errors: {error_count}",
            "errors": errors if errors else None
        }
        
    except Exception as e:
        frappe.db.rollback()
        print(f"Critical error during D2C order import: {str(e)}")
        frappe.log_error(
            title="D2C Order Import - Critical Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return {
            "success": False,
            "message": f"Critical error during D2C order import: {str(e)}"
        }


def create_order_master_record(order_data):
    """
    Create SF Order Master record from D2C order data
    """
    try:
        # Check if order already exists
        existing_order = frappe.get_all(
            "SF Order Master",
            filters={"order_id": order_data.get("order_id")},
            fields=["name"]
        )
        
        if existing_order:
            print(f"Order {order_data.get('order_id')} already exists, skipping...")
            return
        
        # Get or create plant facility
        plant_data = order_data.get("plant", {})
        plant_facility = get_or_create_facility(
            facility_id=plant_data.get("plant_id"),
            facility_name=plant_data.get("plant_name"),
            facility_type="Plant"
        )
        
        # Get or create darkstore facility
        darkstore_data = order_data.get("darkstore", {})
        darkstore_facility = get_or_create_facility(
            facility_id=darkstore_data.get("darkstore_id"),
            facility_name=darkstore_data.get("darkstore_name"),
            facility_latitude=darkstore_data.get("latitude"),
            facility_longitude=darkstore_data.get("longitude"),
            facility_address=darkstore_data.get("address"),
            facility_type="Darkstore"
        )
        
        # Create SF Order Master document
        order_master = frappe.new_doc("SF Order Master")
        
        # Basic order information
        order_master.order_id = order_data.get("order_id")
        order_master.order_type = order_data.get("order_type", "D2C")
        order_master.order_date = order_data.get("order_date")
        
        # Facility details
        order_master.plant = plant_facility
        order_master.darkstore = darkstore_facility
        
        # Delivery location (from darkstore data)
        # if darkstore_data:
        #     order_master.delivery_latitude = darkstore_data.get("latitude")
        #     order_master.delivery_longitude = darkstore_data.get("longitude")
        #     order_master.delivery_address = darkstore_data.get("address")
        
        # Currency default
        order_master.currency = "INR"
        
        # Process SKU summary to create order items
        sku_summary = order_data.get("sku_summary", [])
        total_amount = 0
        
        for sku in sku_summary:
            # Find corresponding SF Product Master
            sf_product = get_sf_product_by_sku(sku.get("sku_id"), sku.get("sku_name"))
            
            if sf_product:
                # Create order item
                order_item = order_master.append("item_table", {})
                order_item.item_id = sku.get("sku_id")
                order_item.item_name = sku.get("sku_name")
                order_item.quantity = sku.get("quantity", 0)
                order_item.unit_price = sf_product.get("offer_price", 0)
                order_item.total_price = order_item.quantity * order_item.unit_price
                order_item.sf_product_master = sf_product.get("name")
                
                total_amount += order_item.total_price
            else:
                print(f"Warning: SF Product Master not found for SKU {sku.get('sku_id')} - {sku.get('sku_name')}")
        
        # Set total amounts
        order_master.subtotal = total_amount
        order_master.total_amount = total_amount
        
        # Save the document
        order_master.insert()
        
        print(f"Created SF Order Master: {order_master.name}")
        
    except Exception as e:
        print(f"Error creating order master record: {str(e)}")
        raise


def get_or_create_facility(facility_id, facility_name, facility_type, facility_latitude = None, facility_longitude = None, facility_address = None):
    """
    Get existing facility or create a new one if not found
    """
    try:
        # Check if facility exists
        existing_facility = frappe.get_all(
            "SF Facility Master",
            filters={
                "facility_id": facility_id,
                "type": facility_type
            },
            fields=["name"]
        )
        
        if existing_facility:
            return existing_facility[0].name
        
        # Create new facility
        print(f"Creating new {facility_type} facility: {facility_id} - {facility_name}")
        
        facility = frappe.new_doc("SF Facility Master")
        facility.facility_id = facility_id
        facility.facility_name = facility_name
        facility.type = facility_type
        facility.latitude = facility_latitude
        facility.longitude = facility_longitude
        facility.address_text = facility_address
        facility.insert()
        
        print(f"Created SF Facility Master: {facility.name}")
        return facility.name
        
    except Exception as e:
        print(f"Error creating facility {facility_id}: {str(e)}")
        raise


def get_sf_product_by_sku(sku_id, sku_name):
    """
    Find SF Product Master by SKU ID or name
    """
    try:
        # First try to find by sf_product_id (assuming it matches sku_id)
        product = frappe.get_all(
            "SF Product Master",
            filters={"sf_product_id": sku_id},
            fields=["name", "offer_price"]
        )
        
        if product:
            return product[0]
        
        # Try to find by variant_full_name (assuming it matches sku_name)
        product = frappe.get_all(
            "SF Product Master",
            filters={"variant_full_name": sku_name},
            fields=["name", "offer_price"]
        )
        
        if product:
            return product[0]
        
        # Try to find by code
        product = frappe.get_all(
            "SF Product Master",
            filters={"code": sku_id},
            fields=["name", "offer_price"]
        )
        
        if product:
            return product[0]
        
        return None
        
    except Exception as e:
        print(f"Error finding SF Product Master for SKU {sku_id}: {str(e)}")
        return None


# Can you write a function to delete all the records from SF Order Master for a given date range
def delete_sf_order_master_records():
    """
    Delete all SF Order Master records for a given date range
    """
    try:
        start_date = '2025-07-15'
        end_date = '2025-07-15'

        # Get count before deleting
        records_to_delete = frappe.get_all(
            "SF Order Master",
            filters={
                "order_date": ["between", [start_date, end_date]]
            },
            fields=["name"]
        )
        
        count = len(records_to_delete)
        
        if count == 0:
            print(f"No SF Order Master records found for date range {start_date} to {end_date}")
            return
        
        # Delete records using proper syntax
        # for record in records_to_delete:
        #     frappe.delete_doc("SF Order Master", record.name)
        #     print(f"Deleted SF Order Master record: {record.name}")
    
        frappe.db.sql(f"DELETE FROM `tabSF Order Master` WHERE `order_date` BETWEEN '{start_date}' AND '{end_date}'")
        
        frappe.db.commit()
        print(f"Deleted {count} SF Order Master records for date range {start_date} to {end_date}")
        
    except Exception as e:
        frappe.db.rollback()
        print(f"Error deleting SF Order Master records: {str(e)}")
        raise

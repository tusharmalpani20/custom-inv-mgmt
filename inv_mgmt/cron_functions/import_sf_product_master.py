import frappe
import requests
from typing import Dict, Any, List
import json
from frappe.utils import now_datetime, get_datetime_str
from datetime import datetime

def format_datetime(date_str: str) -> str:
    """
    Convert API datetime string to ERPNext format (YYYY-MM-DD HH:MM:SS)
    """
    if not date_str:
        return None
    try:
        # Parse the API datetime string
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        # Format it for ERPNext
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            # Try parsing without microseconds
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

def get_api_data(url: str) -> Dict[str, Any]:
    """
    Helper function to fetch data from API
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        frappe.log_error(
            title="SF Product Master API Error",
            message=f"Error fetching data from {url}: {str(e)}\n"
        )
        raise

def import_sf_product_master():
    """
    Import products from SF API and create/update SF Product Master records
    """
    try:
        # Start transaction
        frappe.db.begin()

        # First API call to get total count
        
        base_url = frappe.conf.get('import_sf_product_master_url')
        initial_data = get_api_data(f"{base_url}?limit=1&offset=0")
        total_count = initial_data.get("count", 0)

        if not total_count:
            frappe.throw("No products found in the API response")

        # Second API call to get all products
        all_products_data = get_api_data(f"{base_url}?limit={total_count}&offset=0")
        products = all_products_data.get("results", [])

        if not products:
            frappe.throw("No products found in the API response")

        #print the count of products
        frappe.log(f"Total products: {len(products)}")

        success_count = 0
        error_count = 0
        errors = []

        for product in products:
            try:
                # Check if product already exists
                variant_full_name = product.get("variant_full_name")
                if not variant_full_name:
                    raise ValueError("Variant Full Name is required")

                existing_product = frappe.get_all(
                    "SF Product Master",
                    filters={"sf_product_id": product.get("id")},
                    fields=["name"]
                )

                product_doc = None
                if existing_product:
                    product_doc = frappe.get_doc("SF Product Master", existing_product[0].name)
                    print(f"Product already exists: {variant_full_name}")
                else:
                    product_doc = frappe.new_doc("SF Product Master")
                    print(f"Product does not exist: {variant_full_name}")

                # Format datetime fields
                published_at = format_datetime(product.get("published_at"))
                available_from = format_datetime(product.get("available_from"))
                available_upto = format_datetime(product.get("available_upto"))
                created_at = format_datetime(product.get("created_at"))
                updated_at = format_datetime(product.get("updated_at"))

                # Map fields from API response to DocType
                product_doc.update({
                    "variant_full_name": variant_full_name,
                    "sf_product_id": product.get("id"),
                    "code": product.get("code"),
                    "category_name": product.get("category", {}).get("name"),
                    "category_id": product.get("category", {}).get("id"),
                    "brand_cd": product.get("brand_cd"),
                    "city_cd": product.get("city_cd"),
                    "description": product.get("description"),
                    "subject": product.get("subject"),
                    "is_published": product.get("is_published", 0),
                    "published_at": published_at,
                    "always_available": product.get("always_available", 0),
                    "available_from": available_from,
                    "available_upto": available_upto,
                    "offer_price": product.get("offer_price"),
                    "cost_price_amount": product.get("cost_price_amount"),
                    "cgst": product.get("cgst"),
                    "sgst": product.get("sgst"),
                    "igst": product.get("igst"),
                    "measurement": product.get("measurement"),
                    "measurement_unit": product.get("measurement_unit"),
                    "packaging_type": product.get("packaging_type"),
                    "qty_per_tray": product.get("qty_per_tray"),
                    "delivery_lag_in_days": product.get("delivery_lag_in_days"),
                    "hsn_code": product.get("hsn_code"),
                    "milk_quantity": product.get("milk_quantity"),
                    "height_in_cms": product.get("height_in_cms"),
                    "length_in_cms": product.get("length_in_cms"),
                    "width_in_cms": product.get("width_in_cms"),
                    "weight_in_gms": product.get("weight_in_gms"),
                    "image_link": product.get("image_link"),
                    "thumbnail_link": product.get("thumbnail_link"),
                    "is_combo": product.get("is_combo", 0),
                    "is_offer_variant": product.get("is_offer_variant", 0),
                    "is_dashboard_product": product.get("is_dashboard_product", 0),
                    "no_of_items_in_variant": product.get("no_of_items_in_variant"),
                    "created_at": created_at,
                    "updated_at": updated_at
                })

                if product.get("is_combo", 0) == 1:
                    combo_components = product.get("combos_components")
                    if combo_components:
                        product_doc.combo_detail = json.dumps(combo_components)
                    else:
                        product_doc.combo_detail = None

                # Clear any previous error message
                product_doc.error_message = ""
                
                product_doc.save()
                success_count += 1

            except Exception as e:
                error_count += 1
                error_detail = {
                    "variant_full_name": variant_full_name,
                    "error": str(e)
                }
                errors.append(error_detail)
                
                # Try to update the error message in the document
                try:
                    if product_doc:
                        product_doc.error_message = str(e)
                        product_doc.save()
                except:
                    pass

                frappe.log_error(
                    title=f"SF Product Master Import Error - {variant_full_name}",
                    message=f"Error: {str(e)}\nProduct Data: {json.dumps(product, indent=2)}"
                )

        # Commit transaction
        frappe.db.commit()

        return {
            "success": True,
            "message": f"Import completed. Success: {success_count}, Errors: {error_count}",
            "errors": errors if errors else None
        }

    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(
            title="SF Product Master Import - Critical Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return {
            "success": False,
            "message": f"Critical error during import: {str(e)}"
        } 
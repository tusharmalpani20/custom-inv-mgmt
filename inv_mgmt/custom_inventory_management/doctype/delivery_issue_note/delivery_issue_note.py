# Copyright (c) 2025, Hopnet Communications LLP and contributors
# For license information, please see license.txt

"""
Delivery Issue Note DocType

This module handles the creation and management of Delivery Issue Notes, including:
- UOM (Unit of Measure) handling and conversion
- Validation of quantities (missing, damaged, excess)
- Integration with Delivery Notes

Key Features:
1. UOM Conversion: Handles multiple UOMs per item with automatic conversion
2. Quantity Validation: Ensures quantities are valid and within limits
3. Delivery Note Integration: Manages items from delivery notes

Dependencies:
- ERPNext Item DocType
- ERPNext UOM DocType
- ERPNext UOM Conversion Detail DocType
- ERPNext Delivery Note DocType
"""

import frappe
from frappe.model.document import Document
from frappe import _


@frappe.whitelist()
def get_item_uoms(doctype, txt, searchfield, start, page_len, filters):
    """
    Get valid UOMs for an item. This function is used by the UOM link field in the frontend
    to show only valid UOMs for the selected item.

    Implementation Details:
    1. Fetches the stock UOM from Item master
    2. Gets all UOMs from UOM Conversion Detail table
    3. Combines them ensuring stock UOM is always included
    4. Supports search/filtering of UOMs

    Args:
        doctype (str): The DocType (always 'UOM' in this case)
        txt (str): Search text entered by user
        searchfield (str): Field to search in
        start (int): Starting index for pagination
        page_len (int): Number of results per page
        filters (dict): Contains item_code to filter UOMs

    Returns:
        list: List of tuples containing valid UOMs [(uom,), ...]

    Example:
        When called from frontend:
        >>> get_item_uoms("UOM", "", "name", 0, 20, {"item_code": "ITEM-001"})
        [('Nos',), ('Box',), ('Crate',)]
    """
    item_code = filters.get('item_code')
    
    # First get the stock UOM
    stock_uom = frappe.db.get_value('Item', item_code, 'stock_uom')
    
    # Get all UOMs from UOM Conversion Detail
    uoms = frappe.db.sql("""
        SELECT DISTINCT ucd.uom 
        FROM `tabUOM Conversion Detail` ucd
        WHERE ucd.parent = %s
        AND ucd.uom LIKE %s
    """, (item_code, "%%%s%%" % txt))
    
    # Convert to list of tuples with UOM
    result = [(d[0],) for d in uoms]
    
    # Add stock UOM if not already in the list
    if stock_uom and (stock_uom,) not in result:
        result.append((stock_uom,))
    
    return result


class DeliveryIssueNote(Document):
    """
    Delivery Issue Note Document Class

    This class handles the business logic for Delivery Issue Notes, including:
    - Validation of quantities and UOMs
    - Integration with Delivery Notes
    - Automatic quantity calculations and defaults

    Key Methods:
    - validate(): Main validation method called before save
    - validate_quantities(): Ensures quantities are within valid limits
    - get_delivery_note_items(): Fetches and processes items from Delivery Note
    """

    def validate(self):
        """
        Main validation method called before saving the document.
        Handles all validation checks including:
        - Delivery note items validation
        - Excess quantity validation
        - Missing quantity validation
        - Overall quantities validation
        """
        self.validate_delivery_note_items()
        self.validate_excess_qty()
        self.validate_missing_qty()
        self.validate_quantities()

    def validate_quantities(self):
        """
        Validates that the sum of missing and damaged quantities does not exceed
        the sum of delivery note quantity and excess quantity.
        
        For each item that is part of delivery note:
        total_issues (missing + damaged) <= total_available (delivery + excess)
        """
        for item in self.items:
            if item.is_part_of_delivery_note:
                total_issues = (item.missing_qty or 0) + (item.damaged_qty or 0)
                total_available = (item.delivery_note_qty or 0) + (item.excess_qty or 0)

                if total_issues > total_available:
                    frappe.throw(
                        _("Row #{0}: Total of missing and damaged quantities ({1}) cannot exceed delivery note quantity ({2})")
                        .format(item.idx, total_issues, item.delivery_note_qty)
                    )

    def validate_missing_qty(self):
        """
        Ensures that missing quantity is only added to items that are part of the delivery note.
        This prevents users from marking items as missing when they weren't in the original delivery.
        """
        for item in self.items:
            if not item.is_part_of_delivery_note and item.missing_qty > 0:
                frappe.throw(
                    _("Row #{0}: Cannot add missing quantity to items that are not part of the Delivery Note.")
                    .format(item.idx)
                )

    def validate_excess_qty(self):
        """
        Ensures that excess quantity is only added to items that are NOT part of the delivery note.
        This enforces the business logic that excess items should be added as new rows.
        """
        for item in self.items:
            if item.is_part_of_delivery_note and item.excess_qty > 0:
                frappe.throw(
                    _("Row #{0}: Cannot add excess quantity to items that are part of the Delivery Note. Please add a new row for excess items.")
                    .format(item.idx)
                )

    def before_submit(self):
        """
        Handle operations before document submission:
        1. Validates quantities one final time
        2. Sets default quantities for certain fields
        3. Creates stock entries for missing and damaged items
        """
        self.validate_quantities()  # Additional validation before submit
        self.set_default_quantities()
        self.create_stock_entry()

    def on_submit(self):
        """
        Handle document submission. All main operations are done in before_submit.
        """
        pass

    def set_default_quantities(self):
        """
        Sets default quantities when document is submitted:
        - Sets missing_qty to 0 for non-delivery note items
        - Sets excess_qty to 0 for delivery note items
        """
        for item in self.items:
            # For non-delivery note items, set missing_qty to 0
            if not item.is_part_of_delivery_note:
                item.missing_qty = 0
            # For delivery note items, set excess_qty to 0
            if item.is_part_of_delivery_note:
                item.excess_qty = 0

    def validate_delivery_note_items(self):
        """
        Ensures that items from the original delivery note are not removed.
        This validation:
        1. Counts original delivery note items
        2. Counts current items marked as part of delivery note
        3. If any are missing, identifies and reports them
        """
        if self.get("__islocal"):
            return

        # Get the original delivery note
        delivery_note = frappe.get_doc("Delivery Note", self.delivery_note)
        original_items_count = len(delivery_note.items)

        # Count current items marked as part of delivery note
        current_dn_items_count = len([d for d in self.items if d.is_part_of_delivery_note])

        if current_dn_items_count < original_items_count:
            # Find missing items
            original_items = {item.item_code: item.idx for item in delivery_note.items}
            current_items = {item.item_code for item in self.items if item.is_part_of_delivery_note}
            
            missing_items = set(original_items.keys()) - current_items
            missing_items_detail = [
                f"{item_code} (Row #{original_items[item_code]})" 
                for item_code in missing_items
            ]
            
            frappe.throw(
                _("Cannot remove items from Delivery Note. Missing items: {0}")
                .format(", ".join(missing_items_detail))
            )

    @frappe.whitelist()
    def get_delivery_note_items(self):
        """
        Fetches items from the selected delivery note and populates the items table.
        This method:
        1. Clears existing items
        2. Gets delivery note details
        3. Creates new rows for each delivery note item
        4. Sets default values for quantities
        """
        if not self.delivery_note:
            return

        # Clear existing items
        self.items = []
        
        # Get delivery note
        delivery_note = frappe.get_doc("Delivery Note", self.delivery_note)
        
        for dn_item in delivery_note.items:
            # Create new item row
            item = self.append("items", {})
            
            # Copy fields from delivery note item
            item.item_code = dn_item.item_code
            item.item_name = dn_item.item_name
            item.delivery_note_qty = dn_item.qty
            item.uom = dn_item.uom
            item.stock_uom = dn_item.stock_uom
            item.conversion_factor = dn_item.conversion_factor
            item.stock_qty = dn_item.stock_qty
            item.is_part_of_delivery_note = 1
            
            # Initialize issue quantities to 0
            item.missing_qty = 0
            item.damaged_qty = 0
            item.excess_qty = 0

    def create_stock_entry(self):
        """
        Creates separate stock entries on submission to handle:
        1. Moving missing items to branch-specific 'Missing SKU' warehouse
        2. Moving damaged items to branch-specific 'Damaged SKU' warehouse
        
        The source warehouse is determined by:
        - For internal customers: uses set_target_warehouse from delivery note
        - For non-internal customers: uses set_warehouse from delivery note
        
        The target warehouses are determined by the branch of the source warehouse.
        """
        if not (any(item.missing_qty for item in self.items) or 
                any(item.damaged_qty for item in self.items)):
            return

        # Get delivery note details
        delivery_note = frappe.get_doc("Delivery Note", self.delivery_note)
        
        # Determine source warehouse based on customer type
        source_warehouse = (delivery_note.set_target_warehouse 
                          if delivery_note.is_internal_customer 
                          else delivery_note.set_warehouse)

        # Get branch from source warehouse
        warehouse_branch = frappe.get_cached_value("Warehouse", source_warehouse, "custom_branch")
        if not warehouse_branch:
            frappe.throw(_("Source warehouse '{0}' does not have a branch assigned. Please assign a branch to the warehouse before submitting this document.").format(source_warehouse))

        # Get default company from Global Defaults
        default_company = frappe.get_cached_value('Global Defaults', None, 'default_company')
        if not default_company:
            frappe.throw(_("Please set default company in Global Defaults"))

        # Find branch-specific warehouses using proper filters
        missing_warehouse = frappe.db.get_value("Warehouse", {
            "custom_branch": warehouse_branch,
            "parent_warehouse": "Missed SKU - SFPL",
            "is_rejected_warehouse": 1,
            "warehouse_name": ["like", f"%Missing SKU%"]
        }, "name")
        
        damaged_warehouse = frappe.db.get_value("Warehouse", {
            "custom_branch": warehouse_branch,
            "parent_warehouse": "Damaged SKU - SFPL",
            "is_rejected_warehouse": 1,
            "warehouse_name": ["like", f"%Damaged SKU%"]
        }, "name")

        # Verify that branch-specific warehouses exist
        if not missing_warehouse:
            frappe.throw(_("Missing SKU warehouse does not exist for branch '{0}'. Please ensure the branch has been properly set up with a Missing SKU warehouse under 'Missed SKU - SFPL'.").format(warehouse_branch))
        
        if not damaged_warehouse:
            frappe.throw(_("Damaged SKU warehouse does not exist for branch '{0}'. Please ensure the branch has been properly set up with a Damaged SKU warehouse under 'Damaged SKU - SFPL'.").format(warehouse_branch))

        # Create stock entry for missing items
        missing_items = [item for item in self.items if item.missing_qty]
        if missing_items:
            missing_stock_entry = frappe.new_doc("Stock Entry")
            missing_stock_entry.stock_entry_type = "Material Transfer"
            missing_stock_entry.company = default_company
            missing_stock_entry.from_warehouse = source_warehouse
            missing_stock_entry.to_warehouse = missing_warehouse

            for item in missing_items:
                missing_stock_entry.append("items", {
                    "item_code": item.item_code,
                    "qty": item.missing_qty
                })

            missing_stock_entry.save()
            missing_stock_entry.submit()
            # Append to child table using standard Frappe way
            self.append("created_stock_entry_list", {
                "stock_entry": missing_stock_entry.name
            })
            
            frappe.msgprint(_("Stock Entry {0} created for missing items to {1}").format(missing_stock_entry.name, missing_warehouse))

        # Create stock entry for damaged items
        damaged_items = [item for item in self.items if item.damaged_qty]
        if damaged_items:
            damaged_stock_entry = frappe.new_doc("Stock Entry")
            damaged_stock_entry.stock_entry_type = "Material Transfer"
            damaged_stock_entry.company = default_company
            damaged_stock_entry.from_warehouse = source_warehouse
            damaged_stock_entry.to_warehouse = damaged_warehouse

            for item in damaged_items:
                damaged_stock_entry.append("items", {
                    "item_code": item.item_code,
                    "qty": item.damaged_qty
                })

            damaged_stock_entry.save()
            damaged_stock_entry.submit()
            
            # Append to child table using standard Frappe way
            self.append("created_stock_entry_list", {
                "stock_entry": damaged_stock_entry.name
            })
            
            frappe.msgprint(_("Stock Entry {0} created for damaged items to {1}").format(damaged_stock_entry.name, damaged_warehouse))

    def on_cancel(self):
        """
        Handle document cancellation:
        1. Cancel all linked stock entries
        """
        self.cancel_linked_stock_entries()

    def cancel_linked_stock_entries(self):
        """
        Cancels all stock entries linked to this document.
        Shows appropriate messages and handles errors.
        """
        for se in self.created_stock_entry_list:
            if se.stock_entry:
                stock_entry = frappe.get_doc("Stock Entry", se.stock_entry)
                if stock_entry.docstatus == 1:  # Only cancel if submitted
                    try:
                        stock_entry.cancel()
                        frappe.msgprint(_("Stock Entry {0} cancelled").format(stock_entry.name))
                    except Exception as e:
                        frappe.throw(_("Failed to cancel Stock Entry {0}: {1}").format(
                            stock_entry.name, str(e)))

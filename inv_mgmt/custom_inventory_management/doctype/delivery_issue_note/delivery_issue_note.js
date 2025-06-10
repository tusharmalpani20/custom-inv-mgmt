// Copyright (c) 2025, Hopnet Communications LLP and contributors
// For license information, please see license.txt

/**
 * Delivery Issue Note DocType
 * 
 * This JavaScript handles the client-side functionality of the Delivery Issue Note,
 * including:
 * - UOM handling and conversion
 * - Quantity validations
 * - Dynamic field updates
 * - Integration with Delivery Notes
 * 
 * Key Features:
 * 1. UOM Conversion: Automatic conversion factor updates and stock qty calculation
 * 2. Real-time Validation: Immediate feedback on quantity issues
 * 3. Dynamic Updates: Automatic field updates based on user input
 * 
 * Dependencies:
 * - ERPNext Item DocType
 * - ERPNext UOM DocType
 * - ERPNext get_conversion_factor API
 */

frappe.ui.form.on("Delivery Issue Note", {
	/**
	 * Set up initial document state and queries
	 * @param {Object} frm - The current form
	 */
	refresh(frm) {
		// Only show submitted delivery notes in the delivery_note link field
		frm.set_query("delivery_note", function() {
			return {
				filters: {
					docstatus: 1 // 1 means submitted state in Frappe
				}
			};
		});
	},

	/**
	 * Handle delivery note selection
	 * Fetches items from the selected delivery note
	 * @param {Object} frm - The current form
	 */
	delivery_note: function(frm) {
		if(frm.doc.delivery_note) {
			frm.call({
				doc: frm.doc,
				method: 'get_delivery_note_items',
				callback: function(r) {
					frm.refresh_field('items');
				}
			});
		} else {
			frm.clear_table('items');
			frm.refresh_field('items');
		}
	}
});

// Handle row deletion and field updates in child table
frappe.ui.form.on("Delivery Issue Note Item", {
	/**
	 * Prevent deletion of delivery note items
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 * @returns {boolean} - False to prevent deletion
	 */
	before_items_remove: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		if (row.is_part_of_delivery_note) {
			frappe.throw(__("Cannot delete items that are part of the Delivery Note"));
			return false;
		}
	},

	/**
	 * Handle excess quantity changes
	 * Validates that excess qty is only added to non-delivery note items
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	excess_qty: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		if (row.is_part_of_delivery_note && row.excess_qty > 0) {
			frappe.model.set_value(cdt, cdn, 'excess_qty', 0);
			frappe.throw(__("Cannot add excess quantity to items that are part of the Delivery Note. Please add a new row for excess items."));
		}
		validate_quantities(frm, row);
	},

	/**
	 * Handle missing quantity changes
	 * Validates that missing qty is only added to delivery note items
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	missing_qty: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		if (!row.is_part_of_delivery_note && row.missing_qty > 0) {
			frappe.model.set_value(cdt, cdn, 'missing_qty', 0);
			frappe.throw(__("Cannot add missing quantity to items that are not part of the Delivery Note."));
		}
		validate_quantities(frm, row);
	},

	/**
	 * Handle damaged quantity changes
	 * Validates total quantities
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	damaged_qty: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		validate_quantities(frm, row);
	},

	/**
	 * Handle item code selection
	 * Sets up UOM and conversion factor for new items
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	item_code: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		if (!row.is_part_of_delivery_note) {
			frappe.model.set_value(cdt, cdn, 'conversion_factor', 1);
			// Get the item's stock UOM and set both UOMs
			frappe.db.get_value('Item', row.item_code, ['stock_uom', 'name'], (r) => {
				if (r && r.stock_uom) {
					frappe.model.set_value(cdt, cdn, 'stock_uom', r.stock_uom);
					frappe.model.set_value(cdt, cdn, 'uom', r.stock_uom);
					
					// Set query for UOM field to only show valid UOMs
					frm.script_manager.trigger('set_uom_query', cdt, cdn);
				}
			});
		}
	},

	/**
	 * Handle UOM changes
	 * Updates conversion factor when UOM is changed
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	uom: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		if (row.item_code && row.uom) {
			return frappe.call({
				method: 'erpnext.stock.get_item_details.get_conversion_factor',
				args: {
					item_code: row.item_code,
					uom: row.uom
				},
				callback: function(r) {
					if (r.message) {
						frappe.model.set_value(cdt, cdn, 'conversion_factor', r.message.conversion_factor);
					}
				}
			});
		}
	},

	/**
	 * Handle conversion factor changes
	 * Updates stock quantity based on new conversion factor
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	conversion_factor: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		if (row.qty) {
			frappe.model.set_value(cdt, cdn, 'stock_qty', flt(row.qty * row.conversion_factor));
		}
	},

	/**
	 * Handle quantity changes
	 * Updates stock quantity based on new quantity
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	qty: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		if (row.conversion_factor) {
			frappe.model.set_value(cdt, cdn, 'stock_qty', flt(row.qty * row.conversion_factor));
		}
	},

	/**
	 * Set up UOM query
	 * Configures the UOM field to only show valid UOMs for the selected item
	 * @param {Object} frm - The current form
	 * @param {string} cdt - Child DocType
	 * @param {string} cdn - Child DocName
	 */
	set_uom_query: function(frm, cdt, cdn) {
		let row = locals[cdt][cdn];
		
		// Set UOM query to only show UOMs from Item's UOM Conversion Detail
		frm.set_query("uom", "items", function(doc, cdt, cdn) {
			let row = locals[cdt][cdn];
			return {
				query: "inv_mgmt.custom_inventory_management.doctype.delivery_issue_note.delivery_issue_note.get_item_uoms",
				filters: {
					'item_code': row.item_code
				}
			};
		});
	}
});

/**
 * Validate quantities in a row
 * Ensures that missing + damaged qty doesn't exceed delivery + excess qty
 * @param {Object} frm - The current form
 * @param {Object} row - The row being validated
 */
function validate_quantities(frm, row) {
	if (row.is_part_of_delivery_note) {
		let total_issues = (row.missing_qty || 0) + (row.damaged_qty || 0);
		let total_available = (row.delivery_note_qty || 0) + (row.excess_qty || 0);
		
		if (total_issues > total_available) {
			// Reset the last changed value
			let field_to_reset = frm.doc.__unsaved ? frm.doc.__last_sync_on : 'missing_qty';
			frappe.model.set_value(row.doctype, row.name, field_to_reset, 0);
			
			frappe.throw(
				__("Row #{0}: Total of missing and damaged quantities ({1}) cannot exceed delivery note quantity ({2})", 
				[row.idx, total_issues, row.delivery_note_qty])
			);
		}
	}
}


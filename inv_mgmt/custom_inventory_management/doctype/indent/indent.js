// Copyright (c) 2025, Hopnet Communications LLP and contributors
// For license information, please see license.txt

/**
 * Indent Form Scripts
 * 
 * This file handles the client-side logic for the Indent DocType, specifically:
 * 1. Automatic crate calculations when items are added or quantities change
 * 2. Handling of difference and actual quantity calculations
 * 3. Real-time updates of child table fields
 * 
 * Implementation Details:
 * - Uses frappe.model.set_value for child table operations (not frm.set_value)
 * - Calculates crates/loose based on UOM conversion from server
 * - Handles difference as shortfall in production
 * - Updates actual quantity automatically based on difference
 * 
 * Business Logic:
 * - When an item is selected:
 *   a) Sets UOM to item's stock UOM
 *   b) Clears all quantity fields
 * 
 * - When an item is changed/cleared:
 *   a) Resets all quantity fields to 0
 *   b) Clears UOM
 * 
 * - When quantity changes:
 *   a) Fetches crate conversion details from server
 *   b) Calculates number of crates and loose items
 *   c) Initializes difference as 0
 *   d) Sets actual to original quantity
 * 
 * - When difference is entered:
 *   a) Actual quantity is automatically updated
 *   b) Formula: actual = quantity - difference
 */

frappe.ui.form.on("Indent", {
	refresh(frm) {
		frm.set_value("company", frappe.get_doc("Company", frappe.get_doc("Company").get_default_company()).name);
	},
});

frappe.ui.form.on("Indent Item", {
	items_add: function(frm, cdt, cdn) {
		reset_row_values(frm, cdt, cdn);
	},

	sku: function(frm, cdt, cdn) {
		let row = locals[cdt][cdn];
		
		// Reset all values when item is cleared or changed
		reset_row_values(frm, cdt, cdn);
		
		if (row.sku) {
			// Get stock UOM from Item master
			frappe.db.get_value('Item', row.sku, 'stock_uom', (r) => {
				if (r && r.stock_uom) {
					frappe.model.set_value(cdt, cdn, 'uom', r.stock_uom);
				}
			});
		}
		
		calculate_crates_and_loose(frm, cdt, cdn);
	},

	quantity: function(frm, cdt, cdn) {
		calculate_crates_and_loose(frm, cdt, cdn);
	},

	difference: function(frm, cdt, cdn) {
		// When difference changes, update actual quantity
		let row = locals[cdt][cdn];
		if (row.quantity && row.difference) {
			let actual = row.quantity - row.difference;
			frappe.model.set_value(cdt, cdn, "actual", actual);
		}
	}
});

/**
 * Resets all values in a row to their defaults
 * 
 * @param {Object} frm - The form object
 * @param {string} cdt - Child DocType name
 * @param {string} cdn - Child document name
 */
function reset_row_values(frm, cdt, cdn) {
	frappe.model.set_value(cdt, cdn, "uom", '');
	frappe.model.set_value(cdt, cdn, "quantity", 0);
	frappe.model.set_value(cdt, cdn, "crates", 0);
	frappe.model.set_value(cdt, cdn, "loose", 0);
	frappe.model.set_value(cdt, cdn, "difference", 0);
	frappe.model.set_value(cdt, cdn, "actual", 0);
}

/**
 * Calculates crates and loose items for a given row
 * 
 * @param {Object} frm - The form object
 * @param {string} cdt - Child DocType name
 * @param {string} cdn - Child document name
 * 
 * Process:
 * 1. Validates required fields (quantity and sku)
 * 2. Calls server API to get crate conversion details
 * 3. Updates child table fields with calculated values
 * 4. Handles errors and shows appropriate messages
 */
function calculate_crates_and_loose(frm, cdt, cdn) {
	let row = locals[cdt][cdn];
	if (!row.quantity || !row.sku) return;

	frappe.call({
		method: "inv_mgmt.custom_inventory_management.doctype.indent.indent.get_crate_details",
		args: {
			sku: row.sku,
			quantity: row.quantity
		},
		callback: function(r) {
			if (r.message) {
				let result = r.message;
				
				if (result.error) {
					frappe.show_alert({
						message: __('Error calculating crates: ') + result.error,
						indicator: 'red'
					});
					return;
				}

				// Update the row using frappe.model.set_value for child table
				frappe.model.set_value(cdt, cdn, "crates", result.crates);
				frappe.model.set_value(cdt, cdn, "loose", result.loose);
				
				// Initialize difference as 0 and set actual
				frappe.model.set_value(cdt, cdn, "difference", 0);
				frappe.model.set_value(cdt, cdn, "actual", result.actual);
				
				// Refresh the field to show updated values
				frm.refresh_field('items');

				// Show message if any
				if (result.message) {
					frappe.show_alert({
						message: __(result.message),
						indicator: 'orange'
					});
				}
			}
		}
	});
}

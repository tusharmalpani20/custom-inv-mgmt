// Copyright (c) 2025, Hopnet Communications LLP and contributors
// For license information, please see license.txt

// frappe.ui.form.on("SF Demand Plan", {
// 	refresh(frm) {

// 	},
// });

frappe.ui.form.on('SF Demand Plan', {
    async onload(frm) {
        await build_roster_table(frm);
    },

    async refresh(frm) {
        frm.fields_dict.roster_table.$wrapper.empty();
        await build_roster_table(frm);

        // Disable inputs only if document is submitted
        if (frm.doc.docstatus === 1) {
            frm.fields_dict.roster_table.$wrapper.find('input').prop('disabled', true);
        }
    }
});

async function build_roster_table(frm) {
    const startDate = frappe.datetime.str_to_obj(frm.doc.planning_week);
    if (!startDate) return;

    // Clear any default margins from the wrapper
    frm.fields_dict.roster_table.$wrapper.css({
        'margin': '0',
        'padding': '0'
    });

    // Fetch SKU list
    const skuList = await frappe.call({
        method: 'inv_mgmt.custom_inventory_management.api_end_points.item_api.get_sku_items',
        type: 'GET'
    });

    // Fetch holidays
    const holidays = await frappe.call({
        method: 'inv_mgmt.custom_inventory_management.api_end_points.item_api.get_holidays_for_week',
        args: {
            start_date: frappe.datetime.add_days(frm.doc.planning_week, 0),
            end_date: frappe.datetime.add_days(frm.doc.planning_week, 6)
        }
    });

    const holidayMap = {};
    (holidays.message || []).forEach(h => {
        holidayMap[frappe.datetime.str_to_obj(h.holiday_date).toDateString()] = h.description;
    });

    // Prepare a map from existing child table entries
    const demandMap = {};
    (frm.doc.sku_demand_entries || []).forEach(row => {
        demandMap[`${row.item_code}_${row.day_index}`] = row.quantity;
    });

    // Build the HTML table with sticky header
    let table = `<div style="margin-bottom: 15px;">
        <div class="input-group" style="max-width: 400px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 6px; overflow: hidden; border: 1px solid #e9ecef;">
            <span class="input-group-addon" style="background: #f8f9fa; border: none; padding: 12px 16px; display: flex; align-items: center; justify-content: center;">
                <i class="fa fa-search" style="color: #6c757d;"></i>
            </span>
            <input type="text" class="form-control" id="sku-search" placeholder="Search SKUs..." 
                style="border: none; padding: 12px 16px; font-size: 14px; background: white; height: auto;">
        </div>
    </div>
    <div class="table-container" style="max-height: 700px; overflow-y: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border: 1px solid #e9ecef; background: white; margin-top: 0;">
        <table class="table" style="font-size: 14px; margin: 0; width: 100%; border-collapse: collapse;">
        <thead style="position: sticky; top: 0; background: #f8f9fa; z-index: 10;">
        <tr style="background: #f8f9fa; margin: 0;">
        <th style="position: sticky; left: 0; background: #f8f9fa; z-index: 11; border-right: 1px solid #dee2e6; min-width: 200px; padding: 16px; color: #495057; font-weight: 600; text-align: left; margin: 0;">SKU</th>`;
    
    for (let i = 0; i < 7; i++) {
        const dateObj = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i));
        const dateStr = frappe.datetime.obj_to_user(dateObj);
        const dayStr = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dateObj.getDay()];
        table += `<th style="background: #f8f9fa; min-width: 120px; padding: 16px; color: #495057; font-weight: 600; text-align: center; margin: 0;">${dayStr}<br><small style="color: #6c757d;">${dateStr}</small></th>`;
    }
    table += `</tr><tr style="background: #ffffff; margin: 0;" class="holiday-row"><td style="position: sticky; left: 0; background: #ffffff; z-index: 11; border-right: 1px solid #dee2e6; padding: 12px; font-weight: 600; color: #495057; margin: 0;">Holiday</td>`;

    for (let i = 0; i < 7; i++) {
        const d = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i)).toDateString();
        table += `<td style="background: #ffffff; padding: 12px; text-align: center; margin: 0;"><small style="color: #dc3545; font-weight: 500;">${holidayMap[d] || ''}</small></td>`;
    }

    table += `</tr></thead><tbody>`;

    // Body
    skuList.message.forEach((item, index) => {
        const rowBg = index % 2 === 0 ? '#ffffff' : '#f8f9fa';
        table += `<tr class="sku-row" data-sku="${item.item_code.toLowerCase()}" data-name="${item.item_name.toLowerCase()}" style="background: ${rowBg}; margin: 0;">
        <td style="position: sticky; left: 0; background: ${rowBg}; z-index: 1; border-right: 1px solid #dee2e6; padding: 16px; min-width: 200px; margin: 0;">
            <div style="font-weight: 600; color: #495057; margin-bottom: 4px;">${item.item_code}</div>
            <div style="font-size: 12px; color: #6c757d;">${item.item_name}</div>
        </td>`;
        for (let i = 0; i < 7; i++) {
            const key = `${item.item_code}_${i}`;
            const val = demandMap[key] ?? '';
            const disabled = (frm.doc.docstatus === 1) ? 'disabled' : '';
            table += `<td style="padding: 12px; min-width: 120px; text-align: center; vertical-align: middle; margin: 0;">
                <div style="display: flex; justify-content: center; align-items: center;">
                    <input type="number" class="form-control sku-input" 
                    data-item="${item.item_code}" data-day="${i}" value="${val}" min="0" 
                    style="width: 100px; height: 40px; font-size: 14px; border: 1px solid #ced4da; border-radius: 6px; text-align: center; transition: all 0.2s ease; background: white; margin: 0;" ${disabled}>
                </div>
            </td>`;
        }
        table += `</tr>`;
    });

    table += `</tbody></table></div>`;
    frm.fields_dict.roster_table.$wrapper.html(table);

    // Add hover effects and focus styles
    frm.fields_dict.roster_table.$wrapper.find('.sku-input').on('focus', function() {
        $(this).css({
            'border-color': '#80bdff',
            'box-shadow': '0 0 0 0.2rem rgba(0, 123, 255, 0.25)',
            'outline': 'none'
        });
    }).on('blur', function() {
        $(this).css({
            'border-color': '#ced4da',
            'box-shadow': 'none'
        });
    }).on('hover', function() {
        $(this).css('border-color', '#adb5bd');
    });

    // Add search functionality
    frm.fields_dict.roster_table.$wrapper.find('#sku-search').on('input', function() {
        const searchTerm = $(this).val().toLowerCase();
        const rows = frm.fields_dict.roster_table.$wrapper.find('.sku-row');
        
        rows.each(function() {
            const sku = $(this).data('sku');
            const name = $(this).data('name');
            const matches = sku.includes(searchTerm) || name.includes(searchTerm);
            
            if (searchTerm === '' || matches) {
                $(this).show();
            } else {
                $(this).hide();
            }
        });
    });

    // Change handler to update child table
    frm.fields_dict.roster_table.$wrapper.find('.sku-input').on('change', function () {
        const item_code = $(this).data('item');
        const day_index = parseInt($(this).data('day'));
        const quantity = parseFloat($(this).val()) || 0;

        const existing = (frm.doc.sku_demand_entries || []).find(
            row => row.item_code === item_code && row.day_index === day_index
        );

        if (existing) {
            existing.quantity = quantity;
        } else {
            frm.add_child('sku_demand_entries', {
                item_code,
                day_index,
                quantity
            });
        }

        // Mark the form as dirty so Save becomes active
        frm.dirty();
    });
}

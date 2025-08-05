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

    // Build the HTML table
    let table = `<table class="table table-bordered" style="font-size: 13px;">`;

    // Header
    table += `<thead><tr><th>SKU</th>`;
    for (let i = 0; i < 7; i++) {
        const dateObj = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i));
        const dateStr = frappe.datetime.obj_to_user(dateObj);
        const dayStr = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dateObj.getDay()];
        table += `<th>${dayStr}<br><small>${dateStr}</small></th>`;
    }
    table += `</tr><tr><td>Holiday</td>`;

    for (let i = 0; i < 7; i++) {
        const d = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i)).toDateString();
        table += `<td><small style="color:red">${holidayMap[d] || ''}</small></td>`;
    }

    table += `</tr></thead><tbody>`;

    // Body
    skuList.message.forEach(item => {
        table += `<tr><td><b>${item.item_code}</b><br><small>${item.item_name}</small></td>`;
        for (let i = 0; i < 7; i++) {
            const key = `${item.item_code}_${i}`;
            const val = demandMap[key] ?? '';
            const disabled = (frm.doc.docstatus === 1) ? 'disabled' : '';
            table += `<td><input type="number" class="form-control sku-input" 
                data-item="${item.item_code}" data-day="${i}" value="${val}" min="0" style="width:80px;" ${disabled}></td>`;
        }
        table += `</tr>`;
    });

    table += `</tbody></table>`;
    frm.fields_dict.roster_table.$wrapper.html(table);

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

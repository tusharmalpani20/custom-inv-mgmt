app_name = "inv_mgmt"
app_title = "custom_inventory_management"
app_publisher = "Hopnet Communications LLP"
app_description = "Inventory management for Sid\'s Farm"
app_email = "info@hopnet.co.in"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "inv_mgmt",
# 		"logo": "/assets/inv_mgmt/logo.png",
# 		"title": "custom_inventory_management",
# 		"route": "/inv_mgmt",
# 		"has_permission": "inv_mgmt.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/inv_mgmt/css/inv_mgmt.css"
# app_include_js = "/assets/inv_mgmt/js/inv_mgmt.js"

# include js, css files in header of web template
# web_include_css = "/assets/inv_mgmt/css/inv_mgmt.css"
# web_include_js = "/assets/inv_mgmt/js/inv_mgmt.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "inv_mgmt/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "inv_mgmt/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "inv_mgmt.utils.jinja_methods",
# 	"filters": "inv_mgmt.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "inv_mgmt.install.before_install"
# after_install = "inv_mgmt.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "inv_mgmt.uninstall.before_uninstall"
# after_uninstall = "inv_mgmt.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "inv_mgmt.utils.before_app_install"
# after_app_install = "inv_mgmt.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "inv_mgmt.utils.before_app_uninstall"
# after_app_uninstall = "inv_mgmt.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "inv_mgmt.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

override_doctype_class = {
#	"ToDo": "custom_app.overrides.CustomToDo"
	"Branch": "inv_mgmt.overrides.doctypes.branch.CustomBranch"
}

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

scheduler_events = {
    "cron": {
        "0 21 * * *" : [
            #Here we will run the function to import the SF Product Master Records
            "inv_mgmt.cron_functions.import_sf_product_master.import_sf_product_master"
        ],
        "0 23 * * *" : [
            # Alternative: Use wrapper function with extended timeout for 23:00 timing
            "inv_mgmt.cron_functions.import_sf_order_master.enqueue_import_all_orders"
        ],
        "20 23 * * *" :[
            "inv_mgmt.cron_functions.comprehensive_data_processing_cron.enqueue_comprehensive_data_processing_cron" 
        ],
    },
# 	"all": [
# 		"inv_mgmt.tasks.all"
# 	],
# 	"daily": [
# 		"inv_mgmt.tasks.daily"
# 	],
# 	"hourly": [
# 		"inv_mgmt.tasks.hourly"
# 	],
# 	"weekly": [
# 		"inv_mgmt.tasks.weekly"
# 	],
# 	"monthly": [
# 		"inv_mgmt.tasks.monthly"
# 	],
}

# Testing
# -------

# before_tests = "inv_mgmt.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "inv_mgmt.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "inv_mgmt.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["inv_mgmt.utils.before_request"]
# after_request = ["inv_mgmt.utils.after_request"]

# Job Events
# ----------
# before_job = ["inv_mgmt.utils.before_job"]
# after_job = ["inv_mgmt.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"inv_mgmt.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

fixtures = [

        {
            "doctype": "Workflow"
        },
		{
			"doctype": "Workflow State"
		},
        {
            "doctype": "Workflow Action Master"
        },
        {
            "doctype": "Role"
        },
        {
            "doctype": "Role Profile"
        },
        {
            "doctype": "Customer Category"
        },
        {
            "doctype": "Designation",
            "filters": {
                "custom_grade" : ["is", "set"]
            }
        },
        {
            "doctype": "Warehouse"
        }
]
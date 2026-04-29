import frappe

def execute():
    try:
        frappe.db.add_index("Controller Job", ["status", "job_type", "creation"], "idx_controller_job_status_type_creation")
    except Exception as e:
        pass

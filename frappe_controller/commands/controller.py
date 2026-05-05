import click
import frappe
import frappe.commands.scheduler

@click.command("control")
def start_controller():
	"""Start the custom controller process."""
	from frappe_controller.utils.controller import start_controller as _start_controller
	_start_controller()

commands = [
	start_controller,
]

# Monkey-patch the native frappe `bench worker` command to start the faststream worker instead
original_worker_callback = frappe.commands.scheduler.start_worker.callback

def fs_worker_wrapper(**kwargs):
	queue = kwargs.get("queue") or "default"
	if queue in ("low", "medium", "high"):
		from frappe_controller.utils.background_jobs import start_worker
		start_worker(queue)
	else:
		return original_worker_callback(**kwargs)

frappe.commands.scheduler.start_worker.callback = fs_worker_wrapper

import click
import frappe

@click.command("control")
def start_controller():
	"""Start the custom controller process."""
	from frappe_controller.utils.controller import start_controller as _start_controller
	_start_controller()

commands = [
	start_controller,
]

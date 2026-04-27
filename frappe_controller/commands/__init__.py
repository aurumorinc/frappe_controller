def get_commands():
	from .controller import commands as controller_commands
	return controller_commands

commands = get_commands()

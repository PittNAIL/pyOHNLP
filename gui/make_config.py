import tkinter as tk
import json
import os
from tkinter import ttk


def create_config(root):
    def handle_click(event):
        read_from = {
            "read_from": {
                "db_type": dbt_entry.get(),
                "database": db_entry.get(),
                "user": user_entry.get(),
                "password": pass_entry.get(),
                "host": host_entry.get(),
                "input_table": input_table_entry.get(),
                "text_col": text_entry.get(),
                "id_col": id_entry.get(),
                "meta_data": [metadata_entry.get()],
            }
        }

        for widget in root.winfo_children():
            widget.destroy()
        create_write_to(root, read_from)

    window = root
    window.title("Create Config")

    read_from = tk.Label(window, text="Read From Info:")
    dbt_label = tk.Label(window, text="DB Type:")
    dbt_entry = tk.Entry(window)
    db_label = tk.Label(window, text="DB:")
    db_entry = tk.Entry(window)
    user_label = tk.Label(window, text="User:")
    user_entry = tk.Entry(window)
    pass_label = tk.Label(window, text="Password:")
    pass_entry = tk.Entry(window)
    host_label = tk.Label(window, text="Host:")
    host_entry = tk.Entry(window)
    input_table_label = tk.Label(window, text="Input Table:")
    input_table_entry = tk.Entry(window)
    text_label = tk.Label(window, text="Text Column:")
    text_entry = tk.Entry(window)
    id_label = tk.Label(window, text="ID Column:")
    id_entry = tk.Entry(window)
    metadata_label = tk.Label(window, text="Metadata Columns:")
    metadata_entry = tk.Entry(window)

    button = tk.Button(window, text="Click me!")
    button.bind("<Button-1>", handle_click)

    read_from.pack()
    dbt_label.pack()
    dbt_entry.pack()
    db_label.pack()
    db_entry.pack()
    user_label.pack()
    user_entry.pack()
    pass_label.pack()
    pass_entry.pack()
    host_label.pack()
    host_entry.pack()
    input_table_label.pack()
    input_table_entry.pack()
    text_label.pack()
    text_entry.pack()
    id_label.pack()
    id_entry.pack()
    metadata_label.pack()
    metadata_entry.pack()
    button.pack()


def create_write_to(root, data):
    def handle_click(event):
        write_to = {
            "write_to": {
                "db_type": dbt_entry.get(),
                "database": db_entry.get(),
                "user": user_entry.get(),
                "password": pass_entry.get(),
                "host": host_entry.get(),
                "to_table": to_table_entry.get(),
                "to_csv": to_csv_entry.get(),
            },
            "ruleset_dir": f"./RULESETS/{ruleset_combobox.get()}",
            "enact": enact_option_entry.get(),
        }

        with open("user_conf.json", "w") as f:
            json.dump(data | write_to, f)

        print("Config data written to user_conf.json")
        root.quit()

    def update_options(event=None):
        folder_path = "./RULESETS"
        if os.path.isdir(folder_path):
            folders = [
                f for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))
            ]
            ruleset_combobox["values"] = folders
            ruleset_combobox.set("")

    window = root
    window.title("Create Config")

    read_from = tk.Label(window, text="Write to Info:")
    dbt_label = tk.Label(window, text="DB Type:")
    dbt_entry = tk.Entry(window)
    db_label = tk.Label(window, text="DB:")
    db_entry = tk.Entry(window)
    user_label = tk.Label(window, text="User:")
    user_entry = tk.Entry(window)
    pass_label = tk.Label(window, text="Password:")
    pass_entry = tk.Entry(window)
    host_label = tk.Label(window, text="Host:")
    host_entry = tk.Entry(window)
    to_table_label = tk.Label(window, text="To Table:")
    to_table_entry = tk.Entry(window)
    to_csv_label = tk.Label(window, text="To CSV:")
    to_csv_entry = tk.Entry(window)
    ruleset_dir_label = tk.Label(window, text="Ruleset Directory:")
    enact_option_label = tk.Label(window, text="Enact Output Transformation:")
    enact_option_entry = tk.Entry(window)

    ruleset_combobox = ttk.Combobox(window, state="readonly")
    browse_button = tk.Button(window, text="Refresh", command=update_options)

    button = tk.Button(window, text="Save")
    button.bind("<Button-1>", handle_click)

    read_from.pack()
    dbt_label.pack()
    dbt_entry.pack()
    db_label.pack()
    db_entry.pack()
    user_label.pack()
    user_entry.pack()
    pass_label.pack()
    pass_entry.pack()
    host_label.pack()
    host_entry.pack()
    to_table_label.pack()
    to_table_entry.pack()
    to_csv_label.pack()
    to_csv_entry.pack()
    ruleset_dir_label.pack()
    ruleset_combobox.pack()
    enact_option_label.pack()
    enact_option_entry.pack()
    browse_button.pack()
    button.pack()

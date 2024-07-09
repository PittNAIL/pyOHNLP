import tkinter as tk
import json
import os

window = tk.Tk()
read_from = tk.Label(text="Read From Info:")

dbt_label = tk.Label(text="DB Type:")
dbt_entry = tk.Entry()

db_label = tk.Label(text="DB:")
db_entry = tk.Entry()

user_label = tk.Label(text="User:")
user_entry = tk.Entry()

pass_label = tk.Label(text="Password:")
pass_entry = tk.Entry()

host_label = tk.Label(text="Host:")
host_entry = tk.Entry()

text_label = tk.Label(text="Text Column:")
text_entry = tk.Entry()

id_label = tk.Label(text="ID Column:")
id_entry = tk.Entry()

metadata_label = tk.Label(text="Metadata Columns:")
metadata_entry = tk.Entry()

def handle_click(event):
    read_from = {"read_from" :
             {"db_type":dbt_entry.get(),
              "database":db_entry.get(),
              "user":user_entry.get(),
              "password":pass_entry.get(),
              "host":host_entry.get(),
              "text_col":text_entry.get(),
              "id_col":id_entry.get(),
              "metadata":metadata_entry.get()
              }
             }

    with open('example.json', 'a') as f:
        json.dump(read_from, f)

    print("The button was clicked!")

button = tk.Button(text="Click me!")

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
text_label.pack()
text_entry.pack()
id_label.pack()
id_entry.pack()
metadata_label.pack()
metadata_entry.pack()
button.pack()


window.mainloop()

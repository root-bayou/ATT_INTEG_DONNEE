import tkinter as tk
from tkinter import ttk
import mysql.connector
from prettytable import PrettyTable
import prettytable
import ctypes

class DatabaseSearchApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Database Search App")

        self.conn = mysql.connector.connect(
            host='localhost',
            user='client',
            password='',
            database='codes_postaux'
        )
        self.cursor = self.conn.cursor()
        self.create_widgets()

    def get_table_columns(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        columns = [column[0] for column in self.cursor.description]
        self.cursor.fetchall() 
        return columns

    def create_widgets(self):
        
        self.root.attributes("-alpha", 0.8)
        style = ttk.Style()
        style.theme_use("clam")
        
        self.label_search = ttk.Label(self.root, text="Search by postal-code:", font=("Courier", 14))
        self.entry_search = ttk.Entry(self.root, font=("Arial", 12))
        self.combobox_search_type = ttk.Combobox(self.root, values=["Exact", "Range"], font=("Courier", 14), state="readonly")
        self.combobox_search_type.set("Exact")
        
        self.button_search = ttk.Button(self.root, text="Search", command=self.search_database, style="AccentButton.TButton")


        frame = tk.Frame(self.root)
        self.text_results = tk.Text(frame, height=10, width=50, wrap=tk.WORD, font=("Courier", 14))
        self.text_results.config(state=tk.DISABLED)

        scrollbar = ttk.Scrollbar(frame, command=self.text_results.yview)
        self.text_results['yscrollcommand'] = scrollbar.set
        style.configure("Vertical.TScrollbar", troughcolor="#2E2E2E", background="#4CAF50", gripcount=0)
        
        self.label_search.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        self.entry_search.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")
        self.combobox_search_type.grid(row=0, column=2, padx=10, pady=10, sticky="nsew")
        self.button_search.grid(row=0, column=3, padx=10, pady=10, sticky="nsew")

        frame.grid(row=1, column=0, columnspan=4, padx=10, pady=10, sticky="nsew")
        self.text_results.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        
        style.configure("AccentButton.TButton", foreground="white", background="#4CAF50", font=("Courier", 14), padding=10)
        for i in range(4):
            self.root.grid_columnconfigure(i, weight=1)
        self.root.grid_rowconfigure(1, weight=1)

    def search_database(self):
        columns = self.get_table_columns("table_client")
        self.text_results.config(state=tk.NORMAL)
        self.text_results.delete(1.0, tk.END)
        
        search_terms = self.entry_search.get().split(',')
        search_type = self.combobox_search_type.get()
        
        if search_type == "Exact":
            r_conditions = ' OR '.join([f"code_postal LIKE '{term}'" for term in search_terms])
            query = f"SELECT * FROM table_client WHERE {r_conditions}"
            
        elif search_type == "Range":
            try:
                search_term_test = [int(s) for s in search_terms]
                search_term_pars = [s[0:2] for s in search_terms] 
            except ValueError:
                self.text_results.insert(tk.END, f"Invalid postal code.")
                return
            
            r_conditions = ' OR '.join([f"code_postal LIKE '{term}%'" for term in search_term_pars])
            query = f"SELECT * FROM table_client WHERE {r_conditions}"
        else:
            self.text_results.insert(tk.END, f"Invalid search type.")
            return

        self.cursor.execute(query)
        results = self.cursor.fetchall()
        p_table = PrettyTable()
        for i in range(len(columns)):
            p_table.add_column(columns[i], [])
        if results:
            for row in results:
                row_list = []        
                for element in row :
                    row_list.append(str(element))
                
                p_table.add_row(row_list)
                
            p_table.hrules = prettytable.ALL 
            self.text_results.insert(tk.END, f"{p_table}")
        else:
            self.text_results.insert(tk.END, f"No results found :/")
            
        self.text_results.config(state=tk.DISABLED)
        self.cursor.fetchall()

if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseSearchApp(root)

    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    root.geometry(f"{int(screen_width * 0.8)}x{int(screen_height * 0.8)}+{int(screen_width * 0.1)}+{int(screen_height * 0.1)}")
    
    root.mainloop()

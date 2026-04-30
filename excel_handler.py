import pandas as pd

class ExcelHandler:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None

    def read_excel(self):
        self.df = pd.read_excel(self.file_path, engine='openpyxl')
        print(f"成功读取 {len(self.df)} 行数据")

    def get_column_data(self, column_name):
        if self.df is None:
            raise RuntimeError("请先调用 read_excel()")
        if column_name not in self.df.columns:
            raise ValueError(f"列名 '{column_name}' 不存在")
        return self.df[column_name].fillna('').astype(str).tolist()
# test_sheets.py
import sheets

sh = sheets.open_spreadsheet()
ws = sheets.pick_today_worksheet(sh)
df = sheets.parse_sections(ws)
print(df.head(3)[["vendor_num", "vendor_name", "status", "status_a1"]])

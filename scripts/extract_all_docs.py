import pandas as pd
import json
import os
import sys

# 현재 스크립트 위치를 기준으로 경로 설정
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
file_path = os.path.join(base_dir, '키움 REST API 문서.xlsx')
output_base = os.path.join(base_dir, 'docs/kiwoom_api/')

# 파일 존재 여부 확인
if not os.path.exists(file_path):
    print(f"Error: '{file_path}' 파일을 찾을 수 없습니다.")
    sys.exit(1)

def get_category(sheet_name, api_id):
    if api_id.startswith('au'):
        return 'common'
    if api_id.startswith('ka'):
        if '10' in api_id[:4]: return 'rest_api/domestic_stock'
        if '20' in api_id[:4]: return 'rest_api/industry'
        if '30' in api_id[:4]: return 'rest_api/elw'
        if '40' in api_id[:4]: return 'rest_api/etf'
        if '50' in api_id[:4]: return 'rest_api/gold'
        if '00' in api_id[:4]: return 'rest_api/account'
        return 'rest_api/domestic_stock'
    if api_id.startswith('kt'):
        if api_id in ['kt10000', 'kt10001', 'kt10002', 'kt10003']: return 'rest_api/order'
        return 'rest_api/account'
    if len(api_id) <= 2 or api_id in ['0A', '0B', '0C', '0D', '0E', '0F', '0G', '0H', '0I', '0J', '0U', '0g', '0m', '0s', '0u', '0w', '1h']:
        return 'realtime_wss'
    return 'rest_api/other'

def clean_val(val):
    if pd.isna(val) or str(val).lower() == 'nan':
        return ''
    return str(val).strip().replace('\n', ' ')

xl = pd.ExcelFile(file_path)
all_sheets = xl.sheet_names

processed_count = 0
for sheet in all_sheets:
    if sheet == 'API 리스트' or sheet == '오류코드':
        continue
        
    api_id = ''
    if '(' in sheet and ')' in sheet:
        api_id = sheet.split('(')[-1].replace(')', '')
    else:
        api_id = sheet
        if '(' in sheet:
            api_id = sheet.split('(')[-1].replace(')', '')

    # Map REALTIME special cases
    if sheet in ['주문체결(00)', '잔고(04)', '주식기세(0A)', '주식체결(0B)', '주식우선호가(0C)', '주식호가잔량(0D)', '주식시간외호가(0E)', '주식당일거래원(0F)', 'ETF NAV(0G)', '주식예상체결(0H)', '국제금환산가격(0I)', '업종지수(0J)', '업종등락(0U)', '주식종목정보(0g)', 'ELW 이론가(0m)', '장시작시간(0s)', 'ELW 지표(0u)', '종목프로그램매매(0w)', 'VI발동|해제(1h)']:
        api_id = sheet.split('(')[-1].replace(')', '')

    category = get_category(sheet, api_id)
    target_dir = os.path.join(output_base, category)
    os.makedirs(target_dir, exist_ok=True)
    
    df = xl.parse(sheet)
    
    # We'll use the column names to find where the table starts or look for keywords
    info = {
        "api_id": api_id,
        "api_name": sheet.split('(')[0],
        "category": category,
        "method": "",
        "url": "",
        "header_fields": [],
        "request_body": [],
        "response_body": []
    }
    
    # Identify Method and URL from the info section (usually top rows)
    for idx, row in df.iterrows():
        c0 = clean_val(row.iloc[0])
        c1 = clean_val(row.iloc[1])
        c2 = clean_val(row.iloc[2]) if len(row) > 2 else ""
        
        if 'Method' in c0: info['method'] = c1 if c1 and c1!='nan' else c2
        if 'URL' in c0: info['url'] = c1 if c1 and c1!='nan' else c2

    # Identify Mode based on '구분' (Usually column A)
    mode = None
    response_started = False
    
    for idx, row in df.iterrows():
        # Heuristic detection of mode
        c0 = clean_val(row.iloc[0])
        c1 = clean_val(row.iloc[1])
        
        if any(keyword in c0 for keyword in ['Request', '요청']):
            response_started = False
        if any(keyword in c0 for keyword in ['Response', '응답']):
            response_started = True
            
        if 'Header' in c0 or 'Header' in c1:
            mode = 'header'
        elif 'Body' in c0 or 'Body' in c1:
            if not response_started: mode = 'request'
            else: mode = 'response'
            
        element = clean_val(row.iloc[1])
        if not element or element in ['Element', '구분', 'Unnamed', 'nan', 'nan']: continue
        
        # Check if it has enough columns to be a field row
        if len(row) < 3: continue
        
        name = clean_val(row.iloc[2])
        dtype = clean_val(row.iloc[3]) if len(row) > 3 else ""
        required = clean_val(row.iloc[4]) if len(row) > 4 else ""
        desc = clean_val(row.iloc[6]) if len(row) > 6 else ""
        
        field = {"element": element, "name": name, "type": dtype, "required": required, "description": desc}
        
        if mode == 'header': info['header_fields'].append(field)
        elif mode == 'request': info['request_body'].append(field)
        elif mode == 'response': info['response_body'].append(field)

    md_path = os.path.join(target_dir, f"{api_id}.md")
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# [{api_id}] {info['api_name']}\n\n")
        f.write(f"## 개요\n- **Category**: {category}\n- **Method**: {info['method']}\n- **URL**: {info['url']}\n\n")
        
        if info['header_fields']:
            f.write("## 요청 헤더\n| 필드 | 한글명 | Type | 필수 | 설명 |\n| :--- | :--- | :--- | :--- | :--- |\n")
            for h in info['header_fields']:
                f.write(f"| {h['element']} | {h['name']} | {h['type']} | {h['required']} | {h['description']} |\n")
            f.write("\n")
        
        if info['request_body']:
            f.write("## 요청 바디\n| Element | 한글명 | Type | 필수 | 설명 |\n| :--- | :--- | :--- | :--- | :--- |\n")
            for b in info['request_body']:
                f.write(f"| `{b['element']}` | {b['name']} | {b['type']} | {b['required']} | {b['description']} |\n")
            f.write("\n")
        
        if info['response_body']:
            f.write("## 응답 바디\n| Element | 한글명 | Type | 설명 |\n| :--- | :--- | :--- | :--- |\n")
            for r in info['response_body']:
                f.write(f"| `{r['element']}` | {r['name']} | {r['type']} | {r['description']} |\n")
            f.write("\n")
        
        # Simple Example
        if info['request_body']:
            f.write("## 코드 예시\n```json\n// Request\n{\n")
            req_examples = [f'  "{b["element"]}": ""' for b in info['request_body'][:5]]
            f.write(",\n".join(req_examples) + "\n}\n```\n")

    processed_count += 1

print(f"Successfully processed {processed_count} sheets.")

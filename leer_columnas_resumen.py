import pygsheets
from datetime import datetime, timedelta
import psycopg2
import unicodedata

# URLs de los sheets
sheet_urls = {
    "Argentina1": "https://docs.google.com/spreadsheets/d/18XbyZ8NdwGsm3eqoXqyTn7qWeRjEuNsLxIQcscZTy9Y/edit?gid=1650683826#gid=1650683826",
    "Argentina2": "https://docs.google.com/spreadsheets/d/16nGyUJJtn1JxyDA6pI-OAX19rpUX1XPlwdU4gw4pVfk/edit?gid=1650683826#gid=1650683826",
    "España": "https://docs.google.com/spreadsheets/d/10nr7R_rtkUh7DX8uC_dQXkJJSszDd53P-gxnD3Mxi3s/edit?gid=1650683826#gid=1650683826"
}

def serial_to_date(serial):
    try:
        serial = float(serial)
        # Google Sheets/Excel: 1 = 1899-12-31, pero Sheets usa 1899-12-30
        base_date = datetime(1899, 12, 30)
        date = base_date + timedelta(days=serial)
        return date.strftime('%Y-%m-%d')
    except Exception:
        return str(serial)

def process_artist_name(artist):
    if artist.upper().replace(' ', '') == 'HA*ASH':
        return 'HAASH'
    if artist.upper() == 'C.R.O':
        return 'C.R.O'
    artist = ''.join(c for c in unicodedata.normalize('NFD', artist)
                    if unicodedata.category(c) != 'Mn')
    words = artist.strip().split()
    processed_words = []
    for word in words:
        if word:
            processed_word = word.lower().capitalize()
            processed_words.append(processed_word)
    return ' '.join(processed_words)

def get_db_connection():
    print("[LOG] Conectando a la base de datos...")
    try:
        connection = psycopg2.connect(
            host="ep-plain-voice-a47r1b05-pooler.us-east-1.aws.neon.tech",
            database="verceldb",
            user="default",
            password="Rx3Eq5iQwMpl",
            sslmode="require"
        )
        print("[LOG] Conexión exitosa a la base de datos.")
        return connection
    except Exception as e:
        print(f"[ERROR] Error al conectar a PostgreSQL: {e}")
        return None

def update_divisa_ticketera(conn, artista, fecha_show, divisa, ticketera):
    print(f"[LOG] Intentando UPDATE: artista='{artista}', fecha_show='{fecha_show}', divisa='{divisa}', ticketera='{ticketera}'")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tickets
            SET divisa = %s, ticketera = %s
            WHERE artista = %s AND fecha_show = %s
        """, (divisa, ticketera, artista, fecha_show))
        conn.commit()
        updated = cursor.rowcount
        cursor.close()
        if updated:
            print(f"[OK] UPDATE realizado: {artista} | {fecha_show} | divisa={divisa} | ticketera={ticketera}")
        else:
            print(f"[WARN] No se encontró registro para actualizar: {artista} | {fecha_show}")
    except Exception as e:
        print(f"[ERROR] Error al actualizar {artista} - {fecha_show}: {e}")

def update_categoria(conn, artista, fecha_show, categoria):
    print(f"[LOG] Intentando UPDATE categoria: artista='{artista}', fecha_show='{fecha_show}', categoria={categoria}")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tickets
            SET categoria = %s
            WHERE artista = %s AND fecha_show = %s
        """, (categoria, artista, fecha_show))
        conn.commit()
        updated = cursor.rowcount
        cursor.close()
        if updated:
            print(f"[OK] UPDATE categoria realizado: {artista} | {fecha_show} | categoria={categoria}")
        else:
            print(f"[WARN] No se encontró registro para actualizar categoria: {artista} | {fecha_show}")
        return updated > 0
    except Exception as e:
        print(f"[ERROR] Error al actualizar categoria {artista} - {fecha_show}: {e}")
        return False

def get_shows_from_sheet(sheet_url, sheet_name):
    print(f"\n[LOG] Obteniendo shows del sheet: {sheet_name}")
    gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
    sh = gc.open_by_url(sheet_url)
    shows = []
    for idx, wks in enumerate(sh.worksheets()[2:], start=3):
        print(f"[LOG] Procesando hoja secundaria {idx}: {wks.title}")
        artista = process_artist_name(wks.get_value('B1').strip())
        b2_raw = wks.get_value('B2', value_render='UNFORMATTED_VALUE')
        fecha = serial_to_date(b2_raw) if b2_raw not in (None, '') else ''
        print(f"[LOG] Show encontrado: artista='{artista}', fecha='{fecha}'")
        shows.append({'artista': artista, 'fecha': fecha, 'hoja': wks.title})
    print(f"[LOG] Total shows encontrados en {sheet_name}: {len(shows)}")
    return shows

def print_match_and_update(sheet_url, sheet_name, conn):
    print(f"\n[LOG] Procesando sheet: {sheet_name}")
    gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
    sh = gc.open_by_url(sheet_url)
    resumen_wks = sh[0]  # Primer hoja (Resumen A-Z)
    resumen_values = resumen_wks.get_all_values(include_tailing_empty=False)
    print(f"[LOG] Filas leídas de Resumen A-Z: {len(resumen_values)}")
    resumen_rows = []
    for row_idx in range(3, len(resumen_values)):
        row = resumen_values[row_idx]
        a_raw = resumen_wks.get_value(f'A{row_idx+1}', value_render='UNFORMATTED_VALUE')
        a = serial_to_date(a_raw) if a_raw not in (None, '') else ''
        b = process_artist_name(row[1].strip()) if len(row) > 1 else ''
        k = row[10].strip() if len(row) > 10 else ''
        if not a and not b and not k:
            print(f"[LOG] Corte de lectura en fila {row_idx+1} (vacío)")
            break
        resumen_rows.append({
            'fila': row_idx+1,
            'fecha': a,
            'artista': b,
            'moneda': k
        })
    print(f"[LOG] Total filas parseadas de Resumen: {len(resumen_rows)}")
    total_updates = 0
    for idx, wks in enumerate(sh.worksheets()[2:], start=3):
        print(f"[LOG] Procesando hoja secundaria {idx}: {wks.title}")
        artista = process_artist_name(wks.get_value('B1').strip())
        b2_raw = wks.get_value('B2', value_render='UNFORMATTED_VALUE')
        fecha = serial_to_date(b2_raw) if b2_raw not in (None, '') else ''
        b5 = wks.get_value('B5', value_render='UNFORMATTED_VALUE')
        b5 = str(b5).strip() if b5 is not None else ''
        print(f"[LOG] Datos hoja secundaria: B1='{artista}', B2='{fecha}', B5='{b5}'")
        match_found = False
        for resumen in resumen_rows:
            print(f"[LOG] Intentando match: resumen_artista='{resumen['artista']}', resumen_fecha='{resumen['fecha']}' vs hoja_artista='{artista}', hoja_fecha='{fecha}'")
            if resumen['artista'] == artista and resumen['fecha'] == fecha:
                match_found = True
                print(f"[MATCH] Sheet: {sheet_name} | Hoja: {wks.title}")
                print(f"Resumen -> Fila: {resumen['fila']:<4} Fecha: {resumen['fecha']:<12} Artista: {resumen['artista']:<25} Moneda: {resumen['moneda']:<8}")
                print(f"Hoja   -> B1: {artista:<25} B2: {fecha:<12} B5: {b5:<20}")
                print('-' * 90)
                update_divisa_ticketera(conn, artista, fecha, resumen['moneda'], b5)
                total_updates += 1
        if not match_found:
            print(f"[LOG] No hubo match para hoja secundaria: {wks.title}")
    print(f"[LOG] Total updates realizados en {sheet_name}: {total_updates}")

def modo_2_categoria(conn):
    print("\n[LOG] === MODO 2: ACTUALIZACIÓN DE CATEGORÍAS ===")
    
    # Procesar España (categoria = 5)
    print("\n[LOG] Procesando España - categoria = 5")
    shows_espana = get_shows_from_sheet(sheet_urls["España"], "España")
    updates_espana = 0
    for show in shows_espana:
        if update_categoria(conn, show['artista'], show['fecha'], 5):
            updates_espana += 1
    print(f"[LOG] España: {updates_espana} registros actualizados con categoria = 5")
    
    # Obtener shows de Argentina1 y Argentina2
    print("\n[LOG] Obteniendo shows de Argentina1 y Argentina2")
    shows_argentina1 = get_shows_from_sheet(sheet_urls["Argentina1"], "Argentina1")
    shows_argentina2 = get_shows_from_sheet(sheet_urls["Argentina2"], "Argentina2")
    
    # Crear sets para comparación rápida
    set_argentina1 = set((show['artista'], show['fecha']) for show in shows_argentina1)
    set_argentina2 = set((show['artista'], show['fecha']) for show in shows_argentina2)
    
    print(f"[LOG] Shows únicos en Argentina1: {len(set_argentina1)}")
    print(f"[LOG] Shows únicos en Argentina2: {len(set_argentina2)}")
    
    # Solo en Argentina1 (categoria = 3)
    solo_argentina1 = set_argentina1 - set_argentina2
    print(f"\n[LOG] Shows solo en Argentina1: {len(solo_argentina1)} - categoria = 3")
    updates_solo_arg1 = 0
    for artista, fecha in solo_argentina1:
        if update_categoria(conn, artista, fecha, 3):
            updates_solo_arg1 += 1
    
    # Solo en Argentina2 (categoria = 2)
    solo_argentina2 = set_argentina2 - set_argentina1
    print(f"\n[LOG] Shows solo en Argentina2: {len(solo_argentina2)} - categoria = 2")
    updates_solo_arg2 = 0
    for artista, fecha in solo_argentina2:
        if update_categoria(conn, artista, fecha, 2):
            updates_solo_arg2 += 1
    
    # En ambos (categoria = 4)
    en_ambos = set_argentina1 & set_argentina2
    print(f"\n[LOG] Shows en ambos Argentina1 y Argentina2: {len(en_ambos)} - categoria = 4")
    updates_ambos = 0
    for artista, fecha in en_ambos:
        if update_categoria(conn, artista, fecha, 4):
            updates_ambos += 1
    
    print(f"\n[LOG] === RESUMEN MODO 2 ===")
    print(f"España (categoria=5): {updates_espana} updates")
    print(f"Solo Argentina1 (categoria=3): {updates_solo_arg1} updates")
    print(f"Solo Argentina2 (categoria=2): {updates_solo_arg2} updates")
    print(f"Ambos Argentina (categoria=4): {updates_ambos} updates")
    print(f"Total updates: {updates_espana + updates_solo_arg1 + updates_solo_arg2 + updates_ambos}")

if __name__ == "__main__":
    print("=== SELECTOR DE MODO ===")
    print("1. Modo 1: Actualizar divisa y ticketera")
    print("2. Modo 2: Actualizar categorías")
    
    # Ejecutar ambos modos automáticamente, primero 1 y luego 2
    conn = get_db_connection()
    if not conn:
        print("[ERROR] No se pudo conectar a la base de datos. Abortando.")
    else:
        # Modo 1
        print("\n[LOG] === MODO 1: ACTUALIZAR DIVISA Y TICKETERA ===")
        for name, url in sheet_urls.items():
            try:
                print_match_and_update(url, name, conn)
            except Exception as e:
                print(f"[ERROR] Error procesando {name}: {e}")
        # Modo 2
        modo_2_categoria(conn)
        conn.close()
        print("[LOG] Conexión a base de datos cerrada.") 
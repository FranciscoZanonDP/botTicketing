import pygsheets
import psycopg2
import unicodedata

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
    try:
        connection = psycopg2.connect(
            host="ep-plain-voice-a47r1b05-pooler.us-east-1.aws.neon.tech",
            database="verceldb",
            user="default",
            password="Rx3Eq5iQwMpl",
            sslmode="require"
        )
        return connection
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None

def get_existing_show_details_debug(connection, artista, fecha_show, funcion):
    try:
        cursor = connection.cursor()
        
        print(f"    🔎 QUERY DEBUG:")
        print(f"    Parámetros de búsqueda:")
        print(f"      artista: '{artista}' (len: {len(artista)})")
        print(f"      fecha_show: '{fecha_show}' (len: {len(fecha_show)})")
        print(f"      funcion: '{funcion}' (len: {len(funcion)})")
        
        # Primero, buscar registros que coincidan con artista y fecha sin importar función
        cursor.execute("""
            SELECT artista, fecha_show, funcion, 
                   LENGTH(artista) as artista_len,
                   LENGTH(fecha_show) as fecha_len
            FROM tickets 
            WHERE artista = %s 
            AND fecha_show = %s
        """, (artista, fecha_show))
        
        todos_los_registros = cursor.fetchall()
        print(f"    Registros encontrados con artista y fecha: {len(todos_los_registros)}")
        
        for reg in todos_los_registros:
            artista_bd, fecha_bd, funcion_bd, artista_len, fecha_len = reg
            print(f"      - artista: '{artista_bd}' (len: {artista_len})")
            print(f"        fecha: '{fecha_bd}' (len: {fecha_len})")
            print(f"        funcion: '{funcion_bd}' (tipo: {type(funcion_bd).__name__})")
        
        # Ahora la búsqueda original
        cursor.execute("""
            SELECT artista, fecha_show, funcion
            FROM tickets 
            WHERE artista = %s 
            AND fecha_show = %s 
            AND COALESCE(funcion, '') = %s
            LIMIT 1
        """, (artista, fecha_show, funcion if funcion else ''))
        result = cursor.fetchone()
        
        print(f"    Resultado de búsqueda con función '{funcion}': {result}")
        
        if result is None and funcion:
            cursor.execute("""
                SELECT artista, fecha_show, funcion
                FROM tickets 
                WHERE artista = %s 
                AND fecha_show = %s 
                AND funcion IS NULL
                LIMIT 1
            """, (artista, fecha_show))
            result = cursor.fetchone()
            print(f"    Resultado de búsqueda con función NULL: {result}")
            if result is not None:
                return 'NULL'
        
        cursor.close()
        return result is not None
    except Exception as e:
        print(f"Error en la consulta: {e}")
        return False

def debug_benja_torres():
    print("=== DEBUG: BENJA TORRES (DETALLADO) ===")
    print()
    
    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return
    
    sheet_url = "https://docs.google.com/spreadsheets/d/10nr7R_rtkUh7DX8uC_dQXkJJSszDd53P-gxnD3Mxi3s/edit?gid=1650683826#gid=1650683826"
    
    try:
        print("📊 Conectando al sheet de España...")
        gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
        sh = gc.open_by_url(sheet_url)
        wks = sh.worksheet_by_title('Resumen A-Z')
        
        all_values = wks.get_all_values(include_tailing_empty=False)
        
        if len(all_values) > 3:
            row = all_values[3]
            print("🔍 ANALIZANDO FILA 4 DEL SHEET:")
            
            original_date = row[0].strip()
            if '/' in original_date:
                date_parts = original_date.split('/')
                if len(date_parts) == 2:
                    formatted_date = f"2025-{date_parts[1].zfill(2)}-{date_parts[0].zfill(2)}"
                else:
                    formatted_date = '2025-01-01'
            else:
                formatted_date = '2025-01-01'
            
            artista_original = row[1].strip()
            artista_procesado = process_artist_name(artista_original)
            
            print(f"📅 Fecha formateada: '{formatted_date}'")
            print(f"🎤 Artista procesado: '{artista_procesado}'")
            print()
            
            print("🔍 BÚSQUEDA DETALLADA EN BASE DE DATOS:")
            
            # Buscar con función vacía
            print("  1. Buscando con función vacía...")
            existe = get_existing_show_details_debug(conn, artista_procesado, formatted_date, "")
            
            print()
            print("  2. Buscando con función NULL...")
            existe_null = get_existing_show_details_debug(conn, artista_procesado, formatted_date, None)
            
            print()
            print(f"📊 RESULTADO FINAL:")
            print(f"  Con función '': {existe}")
            print(f"  Con función NULL: {existe_null}")
                
        else:
            print("❌ No hay suficientes filas en el sheet")
    
    except Exception as e:
        print(f"❌ Error en el debugging: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    debug_benja_torres() 
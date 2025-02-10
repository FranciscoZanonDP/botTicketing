import pygsheets
import psycopg2
from datetime import datetime, timedelta
from psycopg2 import Error

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
    except Error as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None

def process_artist_name(artist):
    # Casos especiales de normalización
    special_cases = {
        'KANY GARCIA': 'Kany García',
        'ALVARO DIAZ': 'Álvaro Díaz',
        'HA*ASH': 'HAASH',
        'HAASH': 'HAASH',
        'C.R.O': 'C.R.O'
    }
    
    # Primero convertimos a mayúsculas para comparar
    artist_upper = artist.upper().replace(' ', '')
    
    # Verificamos si es un caso especial
    for case, replacement in special_cases.items():
        if artist_upper == case.replace(' ', ''):
            return replacement
    
    # Si no es caso especial, aplicamos la capitalización normal
    return ' '.join(word.lower().capitalize() for word in artist.split())

def get_sheet_shows():
    SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/18PvV89ic4-jV-SdsM2qsSI37AQG_ifCCXAgVBWJP_dY/edit?gid=1650683826#gid=1650683826'
    gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
    sh = gc.open_by_url(SPREADSHEET_URL)
    wks = sh.worksheet_by_title('Resumen')
    all_values = wks.get_all_values(include_tailing_empty=False)
    
    sheet_shows = set()
    sheet_venues = {}
    for row in all_values[3:]:
        if len(row) >= 8 and row[0].strip() and row[1].strip():
            # Process date
            original_date = row[0].strip()
            if '/' in original_date:
                date_parts = original_date.split('/')
                if len(date_parts) == 3:
                    if len(date_parts[2]) == 2:
                        date_parts[2] = '2025'
                    formatted_date = f"{date_parts[2]}-{date_parts[1].zfill(2)}-{date_parts[0].zfill(2)}"
                elif len(date_parts) == 2:
                    formatted_date = f"2025-{date_parts[1].zfill(2)}-{date_parts[0].zfill(2)}"
                else:
                    formatted_date = '2025-01-01'
            else:
                formatted_date = '2025-01-01'
            
            # Process artist name usando la nueva función
            artist = process_artist_name(row[1].strip())
            
            venue = row[3].strip() if len(row) > 3 and row[3].strip() else '-'
            
            sheet_shows.add((artist, formatted_date))
            sheet_venues[(artist, formatted_date)] = venue
    
    return sheet_shows, sheet_venues

def get_db_shows():
    connection = get_db_connection()
    if not connection:
        return set(), {}
    
    try:
        cursor = connection.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT DISTINCT artista, fecha_show::text, venue
            FROM tickets
            WHERE fecha_show >= %s
            ORDER BY fecha_show
        """, (today,))
        
        db_shows = set()
        db_venues = {}
        for row in cursor.fetchall():
            # Aplicamos el mismo procesamiento de nombres a los datos de la BD
            artist = process_artist_name(row[0])
            db_shows.add((artist, row[1]))
            db_venues[(artist, row[1])] = row[2] if row[2] else '-'
        
        cursor.close()
        return db_shows, db_venues
    
    except Error as e:
        print(f"Error al consultar la base de datos: {e}")
        return set(), {}
    finally:
        connection.close()

def delete_missing_shows(db_shows, sheet_shows):
    connection = get_db_connection()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()
        deleted_count = 0
        
        # Shows en BD pero no en Sheet
        missing_in_sheet = db_shows - sheet_shows
        
        print("\nRegistros que serán eliminados:")
        print("-" * 80)
        print(f"{'Artista':<30} {'Fecha':<12}")
        print("-" * 80)
        
        for artist, show_date in sorted(missing_in_sheet, key=lambda x: x[1]):
            print(f"{artist:<30} {show_date:<12}")
            # Eliminar todos los registros para esta combinación de artista y fecha
            cursor.execute("""
                DELETE FROM tickets 
                WHERE artista = %s 
                AND fecha_show::text = %s
            """, (artist, show_date))
            deleted_count += cursor.rowcount
        
        connection.commit()
        print(f"\nSe eliminaron {deleted_count} registros en total.")
        
    except Error as e:
        print(f"Error al eliminar registros: {e}")
        connection.rollback()
    finally:
        connection.close()

def main():
    print("Comparando shows entre la base de datos y el sheet...")
    
    db_shows, db_venues = get_db_shows()
    sheet_shows, sheet_venues = get_sheet_shows()
    
    # Primero mostrar la comparación
    missing_in_sheet = db_shows - sheet_shows
    missing_in_db = sheet_shows - db_shows
    
    print("\n" + "=" * 80)
    if missing_in_sheet:
        print("\nShows encontrados en la base de datos pero NO en el sheet:")
        print("-" * 80)
        print(f"{'Artista':<30} {'Fecha':<12} {'Venue':<30}")
        print("-" * 80)
        for artist, show_date in sorted(missing_in_sheet, key=lambda x: x[1]):
            venue = db_venues.get((artist, show_date), '-')
            print(f"{artist:<30} {show_date:<12} {venue:<30}")
        print(f"\nTotal de shows faltantes en sheet: {len(missing_in_sheet)}")
        
        # Preguntar si desea eliminar
        respuesta = input("\n¿Desea eliminar estos registros? (s/n): ")
        if respuesta.lower() == 's':
            delete_missing_shows(db_shows, sheet_shows)
    else:
        print("\nNo hay shows faltantes en el sheet.")
    
    print("\n" + "=" * 80)
    if missing_in_db:
        print("\nShows encontrados en el sheet pero NO en la base de datos:")
        print("-" * 80)
        print(f"{'Artista':<30} {'Fecha':<12} {'Venue':<30}")
        print("-" * 80)
        for artist, show_date in sorted(missing_in_db, key=lambda x: x[1]):
            venue = sheet_venues.get((artist, show_date), '-')
            print(f"{artist:<30} {show_date:<12} {venue:<30}")
        print(f"\nTotal de shows faltantes en base de datos: {len(missing_in_db)}")
    else:
        print("\nNo hay shows faltantes en la base de datos.")

if __name__ == "__main__":
    main()

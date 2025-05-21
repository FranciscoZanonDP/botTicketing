import pygsheets
import unicodedata
import psycopg2
from psycopg2 import Error
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

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
        'KANY GARCIA': 'Kany Garcia',
        'KANY GARCÍA': 'Kany Garcia',
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
    
    # Si no es caso especial:
    # 1. Remover tildes
    artist = ''.join(c for c in unicodedata.normalize('NFD', artist)
                    if unicodedata.category(c) != 'Mn')
    
    # 2. Aplicar la capitalización normal
    return ' '.join(word.capitalize() for word in artist.lower().split())

def format_date(date_str):
    if '/' in date_str:
        date_parts = date_str.split('/')
        if len(date_parts) == 3:
            if len(date_parts[2]) == 2:
                date_parts[2] = '2025'
            return f"{date_parts[2]}-{date_parts[1].zfill(2)}-{date_parts[0].zfill(2)}"
        elif len(date_parts) == 2:
            return f"2025-{date_parts[1].zfill(2)}-{date_parts[0].zfill(2)}"
    return '2025-01-01'

def check_existence_and_compare(connection, artist, fecha_show, d8_valor, e8_valor, f8_valor):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT capacidad, holdeo, cortesias
            FROM tickets 
            WHERE artista = %s AND fecha_show = %s
        """, (artist, fecha_show))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            capacidad, holdeo, cortesias = result
            capacidad_match = (str(capacidad) == d8_valor)
            holdeo_match = (str(holdeo) == e8_valor)
            cortesias_match = (str(cortesias) == f8_valor)
            return True, capacidad_match, holdeo_match, cortesias_match
        return False, False, False, False
    except Error as e:
        print(f"Error en la consulta: {e}")
        return False, False, False, False

def update_tickets(connection, artist, fecha_show, d8_valor, e8_valor, f8_valor):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            UPDATE tickets
            SET capacidad = %s, holdeo = %s, cortesias = %s
            WHERE artista = %s AND fecha_show = %s
        """, (d8_valor, e8_valor, f8_valor, artist, fecha_show))
        connection.commit()
        cursor.close()
        print(f"Actualizado: Artista: {artist}, Fecha: {fecha_show}")
        return True
    except Error as e:
        print(f"Error al actualizar el registro: {e}")
        connection.rollback()
        return False

def send_email_report(actualizados):
    sender_email = "malbec@daleplay.la"
    receiver_email = "francisco.zanon@daleplay.la"
    password = "gipz izjp uyci qlqv"

    message = MIMEMultipart('alternative')
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = f"Reporte de Actualizaciones - {datetime.now().strftime('%d/%m/%Y')}"

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333333;
                max-width: 800px;
                margin: 0 auto;
            }}
            .header {{
                background-color: #4A90E2;
                color: white;
                padding: 20px;
                text-align: center;
                border-radius: 5px 5px 0 0;
            }}
            .content {{
                padding: 20px;
                background-color: #ffffff;
            }}
            .section {{
                margin-bottom: 30px;
                background-color: #f9f9f9;
                padding: 15px;
                border-radius: 5px;
                border-left: 4px solid #4A90E2;
            }}
            .update-item {{
                color: #2196F3;
                margin: 10px 0;
                padding: 10px;
                background-color: #f8f9ff;
                border-radius: 3px;
            }}
            .footer {{
                text-align: center;
                padding: 20px;
                color: #666666;
                font-size: 12px;
            }}
            .summary {{
                background-color: #e9ecef;
                padding: 15px;
                border-radius: 5px;
                margin: 20px 0;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Reporte de Actualizaciones</h1>
            <p>{datetime.now().strftime('%d de %B de %Y')}</p>
        </div>
        
        <div class="content">
            <div class="summary">
                <h3>Resumen de Actualizaciones</h3>
                <p>Total de Registros Actualizados: {len(actualizados)}</p>
            </div>
    """

    if actualizados:
        html += """
            <div class="section">
                <h2>✅ Registros Actualizados</h2>
        """
        for registro in actualizados:
            html += f"""
                <div class="update-item">
                    <strong>{registro['artista']}</strong>
                    <br>
                    <small>Fecha del show: {registro['fecha']}</small>
                    <br>
                    <small>Nuevos valores - Capacidad: {registro['capacidad']}, 
                           Holdeo: {registro['holdeo']}, 
                           Cortesias: {registro['cortesias']}</small>
                </div>
            """
        html += "</div>"
    else:
        html += """
            <div class="section">
                <h2>ℹ️ Sin Actualizaciones</h2>
                <p>No se realizaron actualizaciones en este período.</p>
            </div>
        """

    html += """
            <div class="footer">
                <p>Este es un correo automático generado por el sistema de Ticketing</p>
                <p>Dale Play Entertainment © 2024</p>
            </div>
        </div>
    </body>
    </html>
    """

    message.attach(MIMEText(html, 'html'))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, password)
        text = message.as_string()
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
        print("Correo enviado exitosamente")
    except Exception as e:
        print(f"Error al enviar correo: {e}")

def clean_number(value):
    """Limpia un valor numérico eliminando puntos y comas"""
    if isinstance(value, str):
        return value.replace('.', '').replace(',', '')
    return str(value)

def recorrer_hojas():
    # URLs de los sheets
    sheet_urls = {
        "Argentina": "https://docs.google.com/spreadsheets/d/18PvV89ic4-jV-SdsM2qsSI37AQG_ifCCXAgVBWJP_dY/edit?gid=139082797#gid=139082797",
        "España": "https://docs.google.com/spreadsheets/d/10nr7R_rtkUh7DX8uC_dQXkJJSszDd53P-gxnD3Mxi3s/edit?gid=1650683826#gid=1650683826"
    }
    
    try:
        # Conectar a la base de datos
        conn = get_db_connection()
        
        for country, sheet_url in sheet_urls.items():
            print(f"\nProcesando el sheet de {country}...")
            
            # Autorizar y abrir el sheet
            gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
            sh = gc.open_by_url(sheet_url)
            
            # Listas para almacenar los registros
            no_existen = []
            no_coinciden = []
            actualizados = []
            
            # Recorrer todas las hojas excepto 'Resumen'
            for wks in sh.worksheets():
                if wks.title != 'Resumen':
                    print(f"\nHoja: {wks.title}")
                    
                    # Obtener valores de las celdas específicas en un solo paso
                    values = wks.get_values('B1', 'F8', value_render='FORMATTED_VALUE')
                    
                    # Extraer los valores individuales de la lista de valores
                    b1_valor = values[0][0] if values else ''
                    b2_valor = values[1][0] if len(values) > 1 else ''
                    d8_valor = clean_number(values[7][2]) if len(values) > 7 and values[7][2].strip() else '0'
                    e8_valor = values[7][3] if len(values) > 7 and values[7][3].strip() else '0'
                    f8_valor = values[7][4] if len(values) > 7 and values[7][4].strip() else '0'
                    
                    # Procesar B1 con las reglas especificadas
                    b1_procesado = process_artist_name(b1_valor)
                    
                    # Formatear B2 a aaaa-mm-dd
                    b2_formatted = format_date(b2_valor)
                    
                    # Verificar existencia y comparar valores
                    existe, capacidad_match, holdeo_match, cortesias_match = check_existence_and_compare(
                        conn, b1_procesado, b2_formatted, d8_valor, e8_valor, f8_valor)
                    
                    print(f"B1: {b1_procesado}")
                    print(f"B2: {b2_formatted}")
                    print(f"D8: {d8_valor}")
                    print(f"E8: {e8_valor}")
                    print(f"F8: {f8_valor}")
                    print(f"Existe: {existe}")
                    
                    if existe:
                        print(f"Capacidad coincide: {capacidad_match}")
                        print(f"Holdeo coincide: {holdeo_match}")
                        print(f"Cortesias coincide: {cortesias_match}")
                        if not capacidad_match or not holdeo_match or not cortesias_match:
                            no_coinciden.append((b1_procesado, b2_formatted, d8_valor, e8_valor, f8_valor))
                    else:
                        # Si no existe, agregar a la lista
                        no_existen.append((b1_procesado, b2_formatted))
            
            # Imprimir los registros que no existen
            if no_existen:
                print("\nRegistros que no existen en la base de datos:")
                for artist, fecha in no_existen:
                    print(f"Artista: {artist}, Fecha: {fecha}")
            else:
                print("\nTodos los registros existen en la base de datos.")
            
            # Imprimir los registros que no coinciden
            if no_coinciden:
                print("\nRegistros que existen pero no coinciden en capacidad, holdeo o cortesias:")
                for artist, fecha, d8_valor, e8_valor, f8_valor in no_coinciden:
                    print(f"Artista: {artist}, Fecha: {fecha}, Capacidad: {d8_valor}, Holdeo: {e8_valor}, Cortesias: {f8_valor}")
            
            # Actualizar los registros que no coinciden
            if conn and no_coinciden:
                for artist, fecha, d8_valor, e8_valor, f8_valor in no_coinciden:
                    if update_tickets(conn, artist, fecha, d8_valor, e8_valor, f8_valor):
                        actualizados.append({
                            'artista': artist,
                            'fecha': fecha,
                            'capacidad': d8_valor,
                            'holdeo': e8_valor,
                            'cortesias': f8_valor
                        })
            
            # Enviar reporte por email
            send_email_report(actualizados)
        
        # Cerrar la conexión a la base de datos AL FINAL
        if conn:
            conn.close()
        
    except Exception as e:
        print(f"\nError al procesar el sheet: {e}")
        # Asegurar que la conexión se cierre incluso si hay error
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    recorrer_hojas() 

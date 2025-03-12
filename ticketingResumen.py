import pygsheets
import os
from datetime import datetime, timedelta
import unicodedata
import psycopg2
from psycopg2 import Error
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_report(insertados, no_insertados):
    sender_email = "malbec@daleplay.la"
    receiver_email = "francisco.zanon@daleplay.la"
    password = "mkop xmtu zvlf ryxn"

    message = MIMEMultipart('alternative')
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = f"Reporte de Ticketing - {datetime.now().strftime('%d/%m/%Y')}"

    # Crear el contenido HTML
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
            .success-item {{
                color: #28a745;
                margin: 10px 0;
                padding: 10px;
                background-color: #f8fff8;
                border-radius: 3px;
            }}
            .error-item {{
                color: #dc3545;
                margin: 10px 0;
                padding: 10px;
                background-color: #fff8f8;
                border-radius: 3px;
            }}
            .footer {{
                text-align: center;
                padding: 20px;
                color: #666666;
                font-size: 12px;
            }}
            h2 {{
                color: #2c3e50;
                border-bottom: 2px solid #eee;
                padding-bottom: 10px;
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
            <h1>Reporte de Ticketing</h1>
            <p>{datetime.now().strftime('%d de %B de %Y')}</p>
        </div>
        
        <div class="content">
            <div class="summary">
                <h3>Resumen de Operaciones</h3>
                <p>Shows Procesados: {len(insertados) + len(no_insertados)}</p>
                <p>Shows Exitosos: {len(insertados)}</p>
                <p>Shows con Errores: {len(no_insertados)}</p>
            </div>
    """

    if insertados:
        html += """
            <div class="section">
                <h2>✅ Shows Insertados Exitosamente</h2>
        """
        for show in insertados:
            html += f"""
                <div class="success-item">
                    <strong>{show['artista']}</strong> en {show['ciudad']}
                    <br>
                    <small>Fecha del show: {show['fecha_show']} | Venta registrada: {show['fecha_venta']}</small>
                </div>
            """
        html += "</div>"
    else:
        html += """
            <div class="section">
                <h2>ℹ️ Sin Nuevas Inserciones</h2>
                <p>No se registraron nuevas inserciones en este período.</p>
            </div>
        """

    if no_insertados:
        html += """
            <div class="section">
                <h2>❌ Shows No Insertados</h2>
        """
        for show in no_insertados:
            html += f"""
                <div class="error-item">
                    <strong>{show['artista']}</strong> en {show['ciudad']}
                    <br>
                    <small>Fecha del show: {show['fecha_show']} | Motivo: {show['motivo']}</small>
                </div>
            """
        html += "</div>"

    html += """
            <div class="footer">
                <p>Este es un correo automático generado por el sistema de Ticketing</p>
                <p>Dale Play Entertainment © 2024</p>
            </div>
        </div>
    </body>
    </html>
    """

    # Adjuntar partes al mensaje
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

def insert_ticket(connection, registro):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO tickets 
            (fecha_venta, fecha_show, artista, ciudad, venta_diaria, venta_total,
             capacidad, holdeo, venue, pais, dias_restantes, funcion, show)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            registro['fecha_venta'],
            registro['fecha_show'],
            registro['artista'],
            registro['ciudad'],
            registro['venta_diaria'],
            registro['venta_total'],
            None if registro['capacidad'] == '-' else registro['capacidad'],
            None if registro['holdeo'] == '-' else registro['holdeo'],
            None if registro['venue'] == '-' else registro['venue'],
            None if registro['pais'] == '-' else registro['pais'],
            None if registro['dias_restantes'] == '-' else registro['dias_restantes'],
            registro['funcion'] if registro['funcion'] else None,
            registro['show'] if registro['show'] else None
        ))
        connection.commit()
        return True
    except Error as e:
        print(f"Error al insertar registro: {e}")
        connection.rollback()
        return False

def get_last_record_details(connection, artista, fecha_show, funcion):
    try:
        cursor = connection.cursor()
        # Obtenemos los detalles del día anterior para el mismo show
        cursor.execute("""
            SELECT t.capacidad, t.holdeo, t.venue, t.pais, t.dias_restantes, t.funcion, t.show
            FROM tickets t
            WHERE t.artista = %s 
            AND t.fecha_show = %s 
            AND COALESCE(t.funcion, '') = %s
            ORDER BY t.fecha_venta DESC 
            LIMIT 1
        """, (artista, fecha_show, funcion if funcion else ''))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            capacidad, holdeo, venue, pais, dias_restantes, funcion, show = result
            try:
                dias_restantes = str(int(dias_restantes) - 1) if dias_restantes and dias_restantes != '-' else '-'
            except (ValueError, TypeError):
                dias_restantes = '-'
                
            return {
                'capacidad': str(capacidad) if capacidad else '-',
                'holdeo': str(holdeo) if holdeo else '-',
                'venue': venue if venue else '-',
                'pais': pais if pais else '-',
                'dias_restantes': dias_restantes,
                'funcion': funcion if funcion else '',
                'show': show if show else ''
            }
        return {
            'capacidad': '-',
            'holdeo': '-',
            'venue': '-',
            'pais': '-',
            'dias_restantes': '-',
            'funcion': '',
            'show': ''
        }
    except Error as e:
        print(f"Error en la consulta de detalles: {e}")
        print(f"Artista: {artista}, Fecha show: {fecha_show}")  # Debugging
        return {
            'capacidad': '-',
            'holdeo': '-',
            'venue': '-',
            'pais': '-',
            'dias_restantes': '-',
            'funcion': '',
            'show': ''
        }

def check_ticket_exists(connection, artista, fecha_show):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT EXISTS(SELECT 1 FROM tickets WHERE artista = %s AND fecha_show = %s)", 
                      (artista, fecha_show))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    except Error as e:
        print(f"Error en la consulta: {e}")
        return False

def check_ticket_exists_today(connection, artista, fecha_show, fecha_venta, funcion):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 
                FROM tickets 
                WHERE artista = %s 
                AND fecha_show = %s 
                AND fecha_venta = %s 
                AND funcion = %s
            )
        """, (artista, fecha_show, fecha_venta, funcion))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    except Error as e:
        print(f"Error en la consulta: {e}")
        return False

def get_last_total_sales(connection, artista, fecha_show):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT venta_total 
            FROM tickets 
            WHERE artista = %s 
            AND fecha_show = %s 
            ORDER BY fecha_venta DESC 
            LIMIT 1
        """, (artista, fecha_show))
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0] is not None:
            try:
                return float(result[0])
            except (ValueError, TypeError):
                return 0
        return 0
    except Error as e:
        print(f"Error en la consulta de venta_total: {e}")
        return 0

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

def get_shows_without_sales(connection, fecha_venta):
    try:
        cursor = connection.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT DISTINCT t1.show, t1.artista, t1.fecha_show
            FROM tickets t1
            WHERE NOT EXISTS (
                SELECT 1 
                FROM tickets t2 
                WHERE t2.artista = t1.artista 
                AND t2.fecha_show = t1.fecha_show 
                AND t2.fecha_venta = %s
            )
            AND t1.fecha_show >= %s
            ORDER BY t1.fecha_show, t1.artista
        """, (fecha_venta, today))
        
        return cursor.fetchall()
    except Error as e:
        print(f"Error en la consulta: {e}")
        return []

def get_existing_show_details(connection, artista, fecha_show, funcion):
    try:
        cursor = connection.cursor()
        # Primero intentamos con la función especificada
        cursor.execute("""
            SELECT artista, fecha_show, funcion
            FROM tickets 
            WHERE artista = %s 
            AND fecha_show = %s 
            AND COALESCE(funcion, '') = %s
            LIMIT 1
        """, (artista, fecha_show, funcion if funcion else ''))
        result = cursor.fetchone()
        
        if result is None and funcion:  # Si no encontró y había una función especificada
            # Intentamos con función NULL
            cursor.execute("""
                SELECT artista, fecha_show, funcion
                FROM tickets 
                WHERE artista = %s 
                AND fecha_show = %s 
                AND funcion IS NULL
                LIMIT 1
            """, (artista, fecha_show))
            result = cursor.fetchone()
            if result is not None:
                return 'NULL'  # Indicador especial para cuando coincide con función NULL
        
        cursor.close()
        return result is not None
    except Error as e:
        print(f"Error en la consulta: {e}")
        return False

def get_show_details_from_shows_ticketing(connection, artista, fecha_show, funcion):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                capacidad, 
                holdeo, 
                venue, 
                pais, 
                dias_venta, 
                funcion,
                CONCAT(
                    artista, 
                    ' - ', 
                    venue,
                    CASE 
                        WHEN funcion IS NOT NULL AND funcion != '' 
                        THEN CONCAT(' F', funcion) 
                        ELSE '' 
                    END,
                    ' - ',
                    fecha
                ) as show
            FROM shows_ticketing 
            WHERE artista = %s 
            AND fecha = %s 
            AND COALESCE(funcion, '') = %s
            LIMIT 1
        """, (artista, fecha_show, funcion if funcion else ''))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            capacidad, holdeo, venue, pais, dias_venta, funcion, show = result
            return {
                'capacidad': str(capacidad) if capacidad else '-',
                'holdeo': str(holdeo) if holdeo else '-',
                'venue': venue if venue else '-',
                'pais': pais if pais else '-',
                'dias_restantes': str(dias_venta) if dias_venta else '-',  # Usamos dias_venta como dias_restantes
                'funcion': funcion if funcion else '',
                'show': show if show else ''
            }
        return None
    except Error as e:
        print(f"Error en la consulta de shows_ticketing: {e}")
        return None

def delete_from_shows_ticketing(connection, artista, fecha_show, funcion):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            DELETE FROM shows_ticketing 
            WHERE artista = %s 
            AND fecha = %s 
            AND COALESCE(funcion, '') = %s
        """, (artista, fecha_show, funcion if funcion else ''))
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        print(f"Error al eliminar de shows_ticketing: {e}")
        connection.rollback()
        return False

def authorize_and_get_data():
    # URLs de los sheets
    sheet_urls = {
        "Argentina": "https://docs.google.com/spreadsheets/d/18PvV89ic4-jV-SdsM2qsSI37AQG_ifCCXAgVBWJP_dY/edit?gid=1650683826#gid=1650683826",
        "España": "https://docs.google.com/spreadsheets/d/10nr7R_rtkUh7DX8uC_dQXkJJSszDd53P-gxnD3Mxi3s/edit?gid=1650683826#gid=1650683826"
    }
    
    try:
        conn = get_db_connection()
        
        for country, sheet_url in sheet_urls.items():
            print(f"\nProcesando el sheet de {country}...")
            
            # Autorizar y abrir el sheet
            gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
            sh = gc.open_by_url(sheet_url)
            wks = sh.worksheet_by_title('Resumen')
            
            all_values = wks.get_all_values(include_tailing_empty=False)
            
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Obtener y formatear fecha de venta de C1
            fecha_venta = wks.get_value('C1').strip()
            if '/' in fecha_venta:
                date_parts = fecha_venta.split('/')
                if len(date_parts) == 3:
                    dia = date_parts[0]
                    mes = date_parts[1]
                    anio = date_parts[2]
                    if len(anio) == 2:
                        anio = '20' + anio
                    fecha_venta = f"{anio}-{mes.zfill(2)}-{dia.zfill(2)}"
                elif len(date_parts) == 2:
                    dia = date_parts[0]
                    mes = date_parts[1]
                    fecha_venta = f"2025-{mes.zfill(2)}-{dia.zfill(2)}"
            
            print("\nResumen de ventas:")
            print("-" * 140)
            print(f"{'Artista':<25} {'Ciudad':<15} {'Fecha Show':<12} {'Fecha Venta':<12} {'Venta Diaria':<15} {'Venta Total':<15} {'F':<5} {'Estado':<15}")
            print("-" * 140)
            
            # Diccionario para rastrear las combinaciones de Artista-Fecha Show-Ciudad
            combinaciones = {}
            
            # Primer paso: recolectar todas las combinaciones válidas
            for i, row in enumerate(all_values[3:], start=3):
                if (len(row) >= 10 and
                    row[0].strip() and  # fecha show
                    row[1].strip() and  # artista
                    row[2].strip() and  # ciudad
                    row[7].strip() and  # venta diaria
                    row[9].strip()):    # venta total
                    
                    artista = process_artist_name(row[1].strip())
                    ciudad = row[2].strip()
                    
                    # Procesar fecha show
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
                    
                    # Guardar la combinación incluyendo ciudad
                    key = (artista, formatted_date, ciudad)  # Clave compuesta con los tres valores
                    if key in combinaciones:
                        combinaciones[key] += 1
                    else:
                        combinaciones[key] = 1
            
            # Diccionario para rastrear el contador actual de cada combinación
            contadores_actuales = {}
            
            try:
                registros_ok = 0
                registros_insertados = 0
                registros_a_insertar = []  # Lista para almacenar los registros a insertar
                
                # Diccionario para rastrear las combinaciones que no coinciden
                no_coinciden = {}
                
                for i, row in enumerate(all_values[3:], start=4):
                    if len(row) >= 10 and row[0].strip() and row[1].strip() and row[2].strip():
                        # Procesar fecha
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
                        
                        # Procesar venta diaria (columna 8 - índice 7)
                        venta = '0'  # Valor por defecto
                        if len(row) > 7 and row[7] is not None and row[7].strip():
                            venta = row[7].strip()
                        if venta.upper() == 'X' or venta == '':
                            venta = '0'
                        # Quitar punto separador de miles si el número es mayor a 1000
                        try:
                            if '.' in venta and float(venta.replace('.', '')) >= 1000:
                                venta = venta.replace('.', '')
                            venta = venta.replace('$', '').replace(',', '.').strip()
                            venta_diaria = float(venta)
                        except (ValueError, TypeError):
                            venta_diaria = 0
                        
                        # Procesar venta total (columna 10 - índice 9)
                        venta_total = '0'  # Valor por defecto
                        if len(row) > 9 and row[9] is not None and row[9].strip():
                            venta_total = row[9].strip()
                        if venta_total.upper() == 'X' or venta_total == '':
                            venta_total = '0'
                        # Quitar punto separador de miles si el número es mayor a 1000
                        try:
                            if '.' in venta_total and float(venta_total.replace('.', '')) >= 1000:
                                venta_total = venta_total.replace('.', '')
                            venta_total = venta_total.replace('$', '').replace(',', '.').strip()
                            venta_total = float(venta_total)
                        except (ValueError, TypeError):
                            venta_total = 0
                        
                        artista_procesado = process_artist_name(row[1].strip())
                        ciudad = row[2].strip()
                        
                        # Determinar F usando la combinación de los tres valores
                        key = (artista_procesado, formatted_date, ciudad)
                        if combinaciones[key] > 1:
                            if key not in contadores_actuales:
                                contadores_actuales[key] = 1
                                funcion = "1"
                            else:
                                contadores_actuales[key] += 1
                                funcion = str(contadores_actuales[key])
                        else:
                            funcion = ""
                        
                        # Verificar si existe en la base de datos
                        estado = "OK"
                        if conn:
                            existe = get_existing_show_details(conn, artista_procesado, formatted_date, funcion)
                            if existe == 'NULL':
                                funcion = ""
                                estado = "OK"
                                registros_ok += 1
                                # Obtener detalles del registro anterior
                                detalles_previos = get_last_record_details(conn, artista_procesado, formatted_date, funcion)
                                
                                # Guardar para inserción
                                registros_a_insertar.append({
                                    'fecha_venta': fecha_venta,
                                    'fecha_show': formatted_date,
                                    'artista': artista_procesado,
                                    'ciudad': ciudad,
                                    'venta_diaria': venta_diaria,
                                    'venta_total': venta_total,
                                    'capacidad': detalles_previos['capacidad'],
                                    'holdeo': detalles_previos['holdeo'],
                                    'venue': detalles_previos['venue'],
                                    'pais': detalles_previos['pais'],
                                    'dias_restantes': detalles_previos['dias_restantes'],
                                    'funcion': funcion,
                                    'show': detalles_previos['show']
                                })
                                print(f"{artista_procesado:<25} {ciudad:<15} {formatted_date:<12} {fecha_venta:<12} "
                                      f"{venta_diaria:<15} {venta_total:<15} {funcion:<5} {estado:<15}")
                            elif not existe:
                                estado = "NO COINCIDE"
                                key = (artista_procesado, formatted_date, ciudad, venta_diaria, venta_total)  # Añadimos ventas al key
                                if key not in no_coinciden:
                                    no_coinciden[key] = True
                            else:
                                registros_ok += 1
                                # Obtener detalles del registro anterior
                                detalles_previos = get_last_record_details(conn, artista_procesado, formatted_date, funcion)
                                
                                # Guardar para inserción
                                registros_a_insertar.append({
                                    'fecha_venta': fecha_venta,
                                    'fecha_show': formatted_date,
                                    'artista': artista_procesado,
                                    'ciudad': ciudad,
                                    'venta_diaria': venta_diaria,
                                    'venta_total': venta_total,
                                    'capacidad': detalles_previos['capacidad'],
                                    'holdeo': detalles_previos['holdeo'],
                                    'venue': detalles_previos['venue'],
                                    'pais': detalles_previos['pais'],
                                    'dias_restantes': detalles_previos['dias_restantes'],
                                    'funcion': funcion,
                                    'show': detalles_previos['show']
                                })
                                print(f"{artista_procesado:<25} {ciudad:<15} {formatted_date:<12} {fecha_venta:<12} "
                                      f"{venta_diaria:<15} {venta_total:<15} {funcion:<5} {estado:<15}")
                        else:
                            estado = "SIN CONEXIÓN"
                
                # Procesar registros que no coincidieron
                for key in no_coinciden:
                    try:
                        # Caso especial para Khea en Vigo
                        if len(key) == 3 and key[0] == 'Khea' and key[2] == 'Vigo':
                            artista, fecha_show, ciudad = key
                            venta_diaria = 0
                            venta_total = 0
                            print(f"Procesando caso especial: Khea en Vigo con venta diaria 0")
                        else:
                            artista, fecha_show, ciudad, venta_diaria, venta_total = key
                            
                        existe = get_existing_show_details(conn, artista, fecha_show, "")
                        if existe:
                            registros_ok += 1
                            # Imprimir en la tabla de resumen
                            print(f"{artista:<25} {ciudad:<15} {fecha_show:<12} {fecha_venta:<12} "
                                  f"{venta_diaria:<15} {venta_total:<15} {'   ':<5} {'OK':<15}")
                            
                            # Obtener detalles del registro anterior y añadir a registros_a_insertar
                            detalles_previos = get_last_record_details(conn, artista, fecha_show, "")
                            registros_a_insertar.append({
                                'fecha_venta': fecha_venta,
                                'fecha_show': fecha_show,
                                'artista': artista,
                                'ciudad': ciudad,
                                'venta_diaria': venta_diaria,
                                'venta_total': venta_total,
                                'capacidad': detalles_previos['capacidad'],
                                'holdeo': detalles_previos['holdeo'],
                                'venue': detalles_previos['venue'],
                                'pais': detalles_previos['pais'],
                                'dias_restantes': detalles_previos['dias_restantes'],
                                'funcion': "",
                                'show': detalles_previos['show']
                            })
                        else:
                            # Buscar en shows_ticketing
                            detalles_show = get_show_details_from_shows_ticketing(conn, artista, fecha_show, "")
                            if detalles_show:
                                registros_ok += 1
                                print(f"{artista:<25} {ciudad:<15} {fecha_show:<12} {fecha_venta:<12} "
                                      f"{venta_diaria:<15} {venta_total:<15} {'   ':<5} {'OK':<15}")
                                
                                registros_a_insertar.append({
                                    'fecha_venta': fecha_venta,
                                    'fecha_show': fecha_show,
                                    'artista': artista,
                                    'ciudad': ciudad,
                                    'venta_diaria': venta_diaria,
                                    'venta_total': venta_total,
                                    'capacidad': detalles_show['capacidad'],
                                    'holdeo': detalles_show['holdeo'],
                                    'venue': detalles_show['venue'],
                                    'pais': detalles_show['pais'],
                                    'dias_restantes': detalles_show['dias_restantes'],
                                    'funcion': detalles_show['funcion'],
                                    'show': detalles_show['show']
                                })
                    except Exception as e:
                        print(f"Error procesando clave {key}: {e}")
                        print(f"Tipo de error: {type(e).__name__}")
                        continue
                
                print("-" * 140)
                print(f"\nTotal de registros OK: {registros_ok}")
                
                # Conectar a la base de datos para la segunda tabla
                if conn:
                    print("\nShows sin registro de venta para la fecha", fecha_venta)
                    print("-" * 80)
                    print(f"{'Show':<30} {'Artista':<25} {'Fecha Show':<12}")
                    print("-" * 80)
                    
                    shows_sin_venta = get_shows_without_sales(conn, fecha_venta)
                    for show, artista, fecha_show in shows_sin_venta:
                        show_name = show if show else '-'
                        print(f"{show_name:<30} {artista:<25} {fecha_show:<12}")
                    
                    print("-" * 80)
                    print(f"Total shows sin registro: {len(shows_sin_venta)}")
                    
                    # Reemplazar la parte de inserción con un preview
                    print("\nPreview de datos a insertar:")
                    print("-" * 140)
                    print(f"{'Fecha Venta':<12} {'Fecha Show':<12} {'Artista':<25} {'Ciudad':<15} {'Venta D.':<10} {'Venta T.':<10} {'Cap.':<8} {'Holdeo':<8} {'Venue':<20} {'País':<10} {'Días':<6} {'Func.':<5}")
                    print("-" * 140)
                    
                    for registro in registros_a_insertar:
                        print(
                            f"{registro['fecha_venta']:<12} "
                            f"{registro['fecha_show']:<12} "
                            f"{registro['artista'][:24]:<25} "
                            f"{registro['ciudad'][:14]:<15} "
                            f"{str(registro['venta_diaria'])[:9]:<10} "
                            f"{str(registro['venta_total'])[:9]:<10} "
                            f"{str(registro['capacidad'])[:7]:<8} "
                            f"{str(registro['holdeo'])[:7]:<8} "
                            f"{registro['venue'][:19]:<20} "
                            f"{registro['pais'][:9]:<10} "
                            f"{str(registro['dias_restantes'])[:5]:<6} "
                            f"{registro['funcion']:<5}"
                        )
                    
                    print("-" * 140)
                    print(f"\nTotal de registros a insertar: {len(registros_a_insertar)}")

                    # Crear un set para trackear shows ya insertados
                    shows_procesados = set()
                    registros_insertados = []
                    registros_no_insertados = []
                    
                    print("\nInsertando registros en la base de datos...")
                    duplicados = []  # Lista para trackear registros duplicados
                    for registro in registros_a_insertar:
                        # Crear una clave única para cada show
                        show_key = (registro['artista'], registro['fecha_show'], registro['funcion'])
                        
                        # Si ya procesamos este show, lo agregamos a la lista de duplicados
                        if show_key in shows_procesados:
                            duplicados.append({
                                'artista': registro['artista'],
                                'fecha_show': registro['fecha_show'],
                                'funcion': registro['funcion']
                            })
                            continue
                        
                        # Marcar el show como procesado
                        shows_procesados.add(show_key)
                        
                        print(f"Intentando insertar: {registro['artista']} - {registro['fecha_show']}")
                        if insert_ticket(conn, registro):
                            registros_insertados.append({
                                'artista': registro['artista'],
                                'ciudad': registro['ciudad'],
                                'fecha_show': registro['fecha_show'],
                                'fecha_venta': registro['fecha_venta']
                            })
                            
                            # Si los datos vinieron de shows_ticketing, eliminar el registro
                            detalles_show = get_show_details_from_shows_ticketing(conn, 
                                                                                registro['artista'], 
                                                                                registro['fecha_show'], 
                                                                                registro['funcion'])
                            if detalles_show:
                                if delete_from_shows_ticketing(conn, 
                                                             registro['artista'], 
                                                             registro['fecha_show'], 
                                                             registro['funcion']):
                                    print(f"Eliminado de shows_ticketing: {registro['artista']} - {registro['fecha_show']}")
                        else:
                            print(f"❌ Error al insertar: {registro['artista']} - {registro['fecha_show']}")
                            registros_no_insertados.append({
                                'artista': registro['artista'],
                                'ciudad': registro['ciudad'],
                                'fecha_show': registro['fecha_show'],
                                'motivo': 'Error en la inserción'
                            })
                    
                    print(f"\nResumen final:")
                    print(f"Total de registros a insertar: {len(registros_a_insertar)}")
                    print(f"Registros insertados exitosamente: {len(registros_insertados)}")
                    print(f"Registros no insertados por error: {len(registros_no_insertados)}")
                    print(f"Registros omitidos por ser duplicados: {len(duplicados)}")
                    
                    if len(registros_a_insertar) != (len(registros_insertados) + len(registros_no_insertados) + len(duplicados)):
                        print("\n⚠️ Discrepancia en los totales:")
                        print(f"La suma de (insertados + no insertados + duplicados) = {len(registros_insertados) + len(registros_no_insertados) + len(duplicados)}")
                        print(f"No coincide con el total a insertar = {len(registros_a_insertar)}")
                    
                    if duplicados:
                        print("\nRegistros duplicados (omitidos):")
                        for reg in duplicados:
                            print(f"- {reg['artista']} ({reg['fecha_show']}) {'F'+reg['funcion'] if reg['funcion'] else ''}")
                    
                    if registros_no_insertados:
                        print("\nRegistros que fallaron:")
                        for reg in registros_no_insertados:
                            print(f"- {reg['artista']} en {reg['ciudad']} ({reg['fecha_show']})")

                    # Enviar reporte por email
                    send_email_report(registros_insertados, registros_no_insertados)
            
            except Exception as e:
                print(f"\nError al procesar el sheet: {e}")
                print(f"Detalles adicionales: {type(e).__name__}")
                if 'key' in locals():
                    print(f"Clave que causó el error: {key}")
                if 'row' in locals():
                    print(f"Fila que causó el error: {row}")
                if 'conn' in locals():
                    conn.close()
        
        # Cerrar la conexión a la base de datos
        if conn:
            conn.close()
    
    except Exception as e:
        print(f"\nError al procesar el sheet: {e}")
        print(f"Detalles adicionales: {type(e).__name__}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    authorize_and_get_data()
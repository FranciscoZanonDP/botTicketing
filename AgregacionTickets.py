import pygsheets
import psycopg2
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

def get_sheet_data():
    # URLs de los sheets
    sheet_urls = {
        "Argentina": "https://docs.google.com/spreadsheets/d/18PvV89ic4-jV-SdsM2qsSI37AQG_ifCCXAgVBWJP_dY/edit?gid=139082797#gid=139082797",
        "España": "https://docs.google.com/spreadsheets/d/10nr7R_rtkUh7DX8uC_dQXkJJSszDd53P-gxnD3Mxi3s/edit?gid=1650683826#gid=1650683826"
    }
    
    # Preguntar al usuario cuál sheet desea procesar
    print("\nSeleccione el sheet que desea procesar:")
    for idx, country in enumerate(sheet_urls.keys(), 1):
        print(f"{idx}. {country}")
    
    while True:
        try:
            choice = int(input("\nIngrese el número correspondiente al sheet: "))
            if 1 <= choice <= len(sheet_urls):
                selected_country = list(sheet_urls.keys())[choice - 1]
                sheet_url = sheet_urls[selected_country]
                print(f"\nProcesando el sheet de {selected_country}...")
                break
            else:
                print(f"Error: Por favor ingrese un número entre 1 y {len(sheet_urls)}")
        except ValueError:
            print("Error: Por favor ingrese un número válido")
    
    try:
        # Autorizar y abrir el sheet
        gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
        sh = gc.open_by_url(sheet_url)
        
        # Mostrar hojas disponibles con números
        worksheet_list = [wks.title for wks in sh.worksheets()]
        print("\nHojas disponibles:")
        for idx, wks in enumerate(worksheet_list, 1):
            print(f"{idx}. {wks}")
        
        while True:
            try:
                hoja_num = int(input("\nSelecciona el número de la hoja a procesar: "))
                if 1 <= hoja_num <= len(worksheet_list):
                    sheet_name = worksheet_list[hoja_num - 1]
                    confirmacion = input(f"\n¿Estás seguro de seleccionar la hoja '{sheet_name}'? (s/n): ").lower()
                    if confirmacion == 's':
                        wks = sh.worksheet_by_title(sheet_name)
                        break
                    else:
                        print("Ok, elige otra hoja.")
                else:
                    print(f"Error: Por favor ingresa un número entre 1 y {len(worksheet_list)}")
            except ValueError:
                print("Error: Por favor ingresa un número válido")
        
        # Obtener valores automáticamente
        b1_valor = wks.get_value('B1').strip()
        b4_valor = wks.get_value('B4').strip()
        b2_valor = wks.get_value('B2').strip()
        shows = f"{b1_valor}, {b4_valor}; {b2_valor}"  # Combinamos los valores
        
        capacidad = wks.get_value('D8').replace('.', '').replace(',', '')
        holdeo_valor = wks.get_value('E8').strip()
        holdeo = '0' if not holdeo_valor else holdeo_valor.replace('.', '').replace(',', '')
        venue = wks.get_value('B4').strip()
        ciudad = wks.get_value('B3').strip()
        
        dias_venta = 0
        try:
            b8_valor = int(wks.get_value('B8').replace('.', '').replace(',', ''))
            c8_valor = int(wks.get_value('C8').replace('.', '').replace(',', ''))
            dias_venta = b8_valor + c8_valor
        except (ValueError, TypeError):
            print("Error al obtener días de venta, por favor ingrese manualmente")
            while True:
                try:
                    dias_venta = int(input("\nIngresa la cantidad de días de venta: "))
                    if dias_venta <= 0:
                        print("Error: La cantidad de días debe ser mayor a 0")
                        continue
                    break
                except ValueError:
                    print("Error: Por favor ingresa un número válido")

        # Pedir y confirmar datos del evento
        print("\nIngresa los datos del evento:")
        artista = input("Artista: ")
        fecha_show = input("Fecha del show (AAAA-MM-DD): ")
        funcion = input("Función: ")
        pais = input("País: ")

        # Mostrar resumen para confirmación
        print("\nResumen de datos:")
        print(f"Shows: {shows}")
        print(f"Artista: {artista}")
        print(f"Fecha Show: {fecha_show}")
        print(f"Capacidad: {capacidad}")
        print(f"Holdeo: {holdeo}")
        print(f"Venue: {venue}")
        print(f"Ciudad: {ciudad}")
        print(f"Función: {funcion}")
        print(f"País: {pais}")
        print(f"Días de venta: {dias_venta}")

        confirmacion = input("\n¿Los datos son correctos? (s/n): ").lower()
        if confirmacion != 's':
            print("Operación cancelada")
            return

        # Obtener todos los valores
        all_values = wks.get_all_values(include_tailing_empty=False)
        
        # Pedir al usuario el rango de filas
        while True:
            try:
                start_row = int(input("\nIngresa el número de fila inicial: "))
                end_row = int(input("Ingresa el número de fila final: "))
                
                if start_row < 1 or end_row > len(all_values):
                    print(f"\nError: El rango debe estar entre 1 y {len(all_values)}")
                    continue
                
                if start_row > end_row:
                    print("\nError: La fila inicial debe ser menor o igual a la final")
                    continue
                
                break
            except ValueError:
                print("\nError: Por favor ingresa números válidos")
        
        # Ajustar índices (restar 1 porque las listas empiezan en 0)
        start_idx = start_row - 1
        end_idx = end_row
        
        print(f"\nMostrando datos desde la fila {start_row} hasta la fila {end_row}:")
        print(f"A: {artista} | S: {shows} | Fecha Show: {fecha_show}")
        print(f"C: {capacidad} | H: {holdeo} | V: {venue}")
        print(f"C: {ciudad} | F: {funcion} | P: {pais}")
        print("-" * 80)
        print(f"{'Fecha Venta':<15} {'Venta Diaria':<15} {'Venta Total':<15} {'Días Restantes':<15}")
        print("-" * 80)
        
        # Crear conexión a la base de datos
        conn = get_db_connection()
        if not conn:
            print("No se pudo establecer conexión con la base de datos")
            return
        
        cursor = conn.cursor()
        registros_insertados = 0

        # Imprimir los datos de las columnas B, C y E y guardar en DB
        for i, row in enumerate(all_values[start_idx:end_idx]):
            if len(row) >= 5:
                # Procesar la fecha
                fecha_str = row[1].strip()
                if fecha_str:
                    try:
                        # Convertir la fecha al formato deseado
                        if '/' in fecha_str:
                            partes = fecha_str.split('/')
                            if len(partes) == 2:  # Si solo tenemos día y mes
                                dia, mes = partes
                                mes = int(mes)
                                dia = int(dia)
                                
                                # Determinar el año basado en el mes
                                if 1 <= mes <= 4:
                                    anio = "2025"
                                elif 7 <= mes <= 12:
                                    anio = "2024"
                                else:
                                    anio = "2024"  # Default a 2024 si el mes está fuera de rango
                                
                                fecha_venta = f"{anio}-{mes:02d}-{dia:02d}"
                            elif len(partes) == 3:  # Si tenemos día, mes y año
                                dia, mes, anio = partes
                                fecha_venta = f"{anio}-{int(mes):02d}-{int(dia):02d}"
                            else:
                                fecha_venta = fecha_str
                        else:
                            fecha_venta = fecha_str
                    except Exception as e:
                        print(f"Error procesando fecha {fecha_str}: {e}")
                        fecha_venta = fecha_str
                else:
                    fecha_venta = '-'
                
                # Procesar números quitando separadores de miles
                venta_diaria = '0' if row[2].strip() == '' else row[2].replace('.', '').replace(',', '')
                venta_total = row[4].strip()
                if venta_total:
                    venta_total = venta_total.replace('.', '').replace(',', '')
                else:
                    venta_total = '0'
                
                dias_restantes = dias_venta - i
                
                # Imprimir los datos
                print(f"{fecha_venta:<15} {venta_diaria:<15} {venta_total:<15} {dias_restantes:<15}")
                
                try:
                    # Insertar en la base de datos
                    cursor.execute("""
                        INSERT INTO tickets (
                            fecha_venta,
                            venta_diaria,
                            venta_total,
                            artista,
                            fecha_show,
                            capacidad,
                            holdeo,
                            venue,
                            ciudad,
                            funcion,
                            show,
                            dias_restantes,
                            pais
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        fecha_venta,
                        int(venta_diaria),
                        int(venta_total),
                        artista,
                        fecha_show,
                        int(capacidad.replace('.', '').replace(',', '')),
                        int(holdeo),
                        venue,
                        ciudad,
                        funcion,
                        shows,
                        dias_restantes,
                        pais
                    ))
                    registros_insertados += 1
                except Error as e:
                    print(f"Error al insertar registro: {e}")
                    continue
            else:
                print(f"{'(vacío)':<15} {'(vacío)':<15} {'(vacío)':<15} {'(vacío)':<15}")
        
        # Commit y cerrar conexión
        conn.commit()
        print("-" * 80)
        print(f"\nSe insertaron {registros_insertados} registros en la base de datos.")
        
    except Exception as e:
        print(f"\nError al procesar el sheet: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    get_sheet_data()
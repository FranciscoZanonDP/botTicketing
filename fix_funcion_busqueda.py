# FIX PARA EL PROBLEMA DE BENJA TORRES
# Este código debe reemplazar la función get_existing_show_details en ticketingResumen.py

def get_existing_show_details(connection, artista, fecha_show, funcion):
    try:
        cursor = connection.cursor()
        
        # Si la función está vacía, buscar CUALQUIER registro con ese artista y fecha
        if not funcion or funcion == '':
            cursor.execute("""
                SELECT artista, fecha_show, funcion
                FROM tickets 
                WHERE artista = %s 
                AND fecha_show = %s 
                LIMIT 1
            """, (artista, fecha_show))
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        
        # Si hay una función específica, buscar con esa función exacta
        cursor.execute("""
            SELECT artista, fecha_show, funcion
            FROM tickets 
            WHERE artista = %s 
            AND fecha_show = %s 
            AND COALESCE(funcion, '') = %s
            LIMIT 1
        """, (artista, fecha_show, funcion))
        result = cursor.fetchone()
        
        if result is None:
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
                return 'NULL'
        
        cursor.close()
        return result is not None
    except Exception as e:
        print(f"Error en la consulta: {e}")
        return False

# APLICAR ESTE CAMBIO:
# 1. Ir a ticketingResumen.py línea ~360
# 2. Reemplazar la función get_existing_show_details con la de arriba
# 3. El problema se resolverá 
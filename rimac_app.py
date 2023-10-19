from flask import Flask, request, jsonify
import pyodbc
import bcrypt
import os
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

db_config = {
    'Driver': '{ODBC Driver 17 for SQL Server}',  
    'Server': '171.15.10.9',
    'Database': 'SGD_PCM',
    'UID': 'sa',
    'PWD': 'averigualo2050$',
    'Encrypt': 'no'
}

# Establish database connection
def connect_to_database():
    try:
        conn = pyodbc.connect(**db_config)
        print('Connected to SQL Server')
        return conn
    except pyodbc.Error as e:
        print('Error connecting to SQL Server:', str(e))
        return None

@app.route('/', methods=['GET'])
def hello():
    return jsonify({'message': 'El backend está en funcionamiento.'}), 200
#########  REGISTER  #########
@app.route('/register', methods=['POST'])
def register():
    req_data = request.json

    # Extract data from request
    usuario = req_data['usuario']
    contrasena = req_data['contrasena']
    nombre = req_data['nombre']
    apellidoPat = req_data['apellidoPat']
    apellidoMat = req_data['apellidoMat']
    fechaNacimiento = req_data['fechaNacimiento']
    codigoDependencia = req_data['codigoDependencia']

    try:
        if not all([nombre, apellidoPat, apellidoMat, fechaNacimiento, codigoDependencia, usuario, contrasena]):
            return jsonify({'error': 'Por favor, complete todos los campos.'}), 400

        ROL_P = 2

        contrasenaEncriptada = bcrypt.hashpw(contrasena.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        query1 = f"""
            INSERT INTO M_USUA_COM (NOMBRE, APELLIDO_PAT, APELLIDO_MAT, FECHA_NACIMIENTO, COD_DEPEN)
            VALUES ('{nombre}', '{apellidoPat}', '{apellidoMat}', '{fechaNacimiento}', '{codigoDependencia}');
        """

        query2 = f"""
            INSERT INTO M_USUA_NEW_ENCRY (USUARIO, CONTRASENA, COD_DEPEN, ROL_P)
            VALUES ('{usuario}', '{contrasenaEncriptada}', '{codigoDependencia}', '{ROL_P}');
        """

        try:
            conn = connect_to_database()
            cursor = conn.cursor()
            print('Query 1:', query1)
            cursor.execute(query1)
            print('Query 2:', query2)
            cursor.execute(query2)
            conn.commit()
            cursor.close()
            conn.close()
            return jsonify({'message': 'Usuario registrado exitosamente.'}), 200
        except pyodbc.Error as e:
            print('Error during registration:', str(e))
            return jsonify({'error': 'Hubo un error al registrar el usuario.'}), 500
    except Exception as e:
        print('Error during registration:', str(e))
        return jsonify({'error': 'Hubo un error al registrar el usuario.'}), 500

#########  LOGIN  #########
@app.route('/login', methods=['POST'])
def login():
    req_data = request.json

    usuario = req_data['usuario']
    contrasena = req_data['contrasena']

    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM M_USUA_NEW_ENCRY WHERE USUARIO = ?", (usuario,))
        result = cursor.fetchone()

        if result is None:
            return jsonify({'success': False, 'message': 'Credenciales incorrectas'}), 401

        hashed_password = result.CONTRASENA
        passwords_match = bcrypt.checkpw(contrasena.encode('utf-8'), hashed_password.encode('utf-8'))

        if passwords_match:
            COD_DEPEN = result.COD_DEPEN
            ROL_P = result.ROL_P

            return jsonify({
                'success': True,
                'message': 'Login exitoso',
                'COD_DEPEN': COD_DEPEN,
                'ROL_P': ROL_P
            }), 200
        else:
            return jsonify({'success': False, 'message': 'Credenciales incorrectas'}), 401
    except pyodbc.Error as e:
        print('Error during login:', str(e))
        return jsonify({'success': False, 'message': 'Error en el servidor'}), 500
    finally:
        cursor.close()
        conn.close()

#########  CONSULTA  #########
@app.route('/consulta', methods=['POST'])
def consulta():
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        query = """
            SELECT TOP 100
            A.NU_ANN,
            A.NU_EMI,
            B.NU_EXPEDIENTE, 
            CONVERT(VARCHAR(10), A.FE_EMI, 103) FE_EMI_CORTA,
            A.ES_DOC_EMI,
            A.CO_GRU,
            A.DE_ASU,
            A.CO_DEP_EMI, 
            J.DE_DEPENDENCIA AS DEPEND_EMIS,
            A.CO_EMP_EMI,
            RTRIM(G.CEMP_APEPAT)+' '+ RTRIM(G.CEMP_APEMAT)+' '+ RTRIM(G.CEMP_DENOM) AS NOMBRE_PERSONAL_EMI,
            A.CO_OTR_ORI_EMI,
            A.CO_TIP_DOC_ADM,
            D.CDOC_DESDOC,
            A.NU_DOC_EMI,
            A.DE_DOC_SIG,
            B.IN_EXISTE_DOC EXISTE_DOC, 
            A.TI_EMI, 
            A.NU_RUC_EMI,  
            COALESCE(A.NU_DNI_EMI, CAST(A.NU_EMI AS VARCHAR)) AS IDENTIFICACION,
            RTRIM(H.DEAPP)+' ' +RTRIM(H.DEAPM)+ ' '+RTRIM(H.DENOM) AS NOMBRE_REMITENTE,
            A.NU_CANDES, 
            A.ES_DOC_EMI, 
            A.CO_LOC_EMI,  
            A.CO_DEP, 
            A.NU_COR_EMI, 
            C.CELE_DESELE,
            E.TI_DES,
            E.CO_EMP_DES,
            E.CO_DEP_DES,
            I.DE_DEPENDENCIA AS DEPEND_RECEP,
            RTRIM(F.CEMP_APEPAT) +' '+ RTRIM(F.CEMP_APEMAT)+' '+RTRIM(F.CEMP_DENOM) AS NOMBRE_COMPLETO_DEST,
            ROW_NUMBER() OVER (ORDER BY A.NU_COR_EMI) AS ROWNUM 
        FROM 
            [SGD_PCM].[IDOSGD].[TDTV_REMITOS] A 
        INNER JOIN 
            [SGD_PCM].[IDOSGD].[TDTX_REMITOS_RESUMEN] B ON B.NU_ANN=A.NU_ANN AND B.NU_EMI=A.NU_EMI
        LEFT JOIN
            [SGD_PCM].[IDOSGD].[RHTM_DEPENDENCIA] AS J ON J.CO_DEPENDENCIA=A.CO_DEP_EMI
        INNER JOIN 
            [SGD_PCM].[IDOSGD].[TDTV_DESTINOS] AS E ON E.NU_EMI=A.NU_EMI
        INNER JOIN 
            [SGD_PCM].[IDOSGD].[SI_ELEMENTO] C ON C.CELE_CODELE=A.TI_EMI
        INNER JOIN 
            [SGD_PCM].[IDOSGD].[SI_MAE_TIPO_DOC] AS D ON D.CDOC_TIPDOC=A.CO_TIP_DOC_ADM
        INNER JOIN 
            [SGD_PCM].[IDOSGD].[RHTM_PER_EMPLEADOS] AS F ON F.CEMP_CODEMP=E.CO_EMP_DES
        INNER JOIN 
            [SGD_PCM].[IDOSGD].[RHTM_PER_EMPLEADOS] AS G ON G.CEMP_CODEMP=A.CO_EMP_EMI
        LEFT JOIN 
            [SGD_PCM].[IDOSGD].[TDTX_ANI_SIMIL] AS H ON H.NULEM=A.NU_DNI_EMI
        INNER JOIN 
            [SGD_PCM].[IDOSGD].[RHTM_DEPENDENCIA] AS I ON I.CO_DEPENDENCIA=E.CO_DEP_DES

        WHERE 
            A.NU_ANN = 2023
            AND A.TI_EMI<>01
            AND A.CO_DEP_EMI =00032 
            AND CTAB_CODTAB='TIP_DESTINO' 
            AND A.CO_GRU = '3' 
            AND A.ES_ELI=0
            ORDER BY NU_EMI DESC
        """

        cursor.execute(query)
        result = cursor.fetchall()

        dataToSendToFrontend = [
            {
                'NU_EXPEDIENTE': record.NU_EXPEDIENTE,
                'DE_ASU': record.DE_ASU,
                'FE_EMI_CORTA': record.FE_EMI_CORTA,
                'DEPEND_EMIS': record.DEPEND_EMIS,
                'CDOC_DESDOC': record.CDOC_DESDOC,
                'IDENTIFICACION': record.IDENTIFICACION,
                'NOMBRE_REMITENTE': record.NOMBRE_REMITENTE or '',
                'DEPEND_RECEP': record.DEPEND_RECEP,
                'NOMBRE_COMPLETO_DEST': record.NOMBRE_COMPLETO_DEST,
            }
            for record in result
        ]

        conn.close()
        return jsonify({'data': dataToSendToFrontend})

    except pyodbc.Error as e:
        print('Error durante la consulta:', str(e))
        return jsonify({'message': 'Error en el servidor'}), 500
    
#########  CONSULTA:codigoDependencia  #########
@app.route('/consulta/<codDep>', methods=['GET'])
def consulta_codDep(codDep):
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        query = f"""
            SELECT 
            NU_EXPEDIENTE, 
            DE_ASU, 
            FE_EMI_CORTA, 
            DEPEND_EMIS, 
            IDENTIFICACION + '-' + NOMBRE_REMITENTE AS REMITENTE, 
            DEPEND_RECEP, 
            NOMBRE_COMPLETO_DEST,
            TIPO_ESTA,
            OBSER
        FROM 
            EXPE_SGD_NEW_2
        LEFT JOIN 
            ESTA_DOC AS EST ON EST.ESTA_ID = EXPE_SGD_NEW_2.ESTADO
        WHERE 
            EXPE_SGD_NEW_2.CO_DEP_DES = {codDep} OR 
            DEPEND_RECEP = (SELECT DE_DEPENDENCIA FROM [SGD_PCM].[IDOSGD].[RHTM_DEPENDENCIA] WHERE CO_DEPENDENCIA={codDep})
        ORDER BY CONVERT(DATETIME, FE_EMI_CORTA, 103) DESC
        """

        cursor.execute(query)
        result = cursor.fetchall()

        conn.close()

        data_to_send_to_frontend = []

        for record in result:
            data_to_send_to_frontend.append({
                'NU_EXPEDIENTE': record.NU_EXPEDIENTE,
                'DE_ASU': record.DE_ASU,
                'FE_EMI_CORTA': record.FE_EMI_CORTA,
                'DEPEND_EMIS': record.DEPEND_EMIS,
                'REMITENTE': record.REMITENTE,
                'DEPEND_RECEP': record.DEPEND_RECEP,
                'NOMBRE_COMPLETO_DEST': record.NOMBRE_COMPLETO_DEST,
                'TIPO_ESTA': record.TIPO_ESTA,
                'OBSER': record.OBSER,
            })

        return jsonify(data_to_send_to_frontend)

    except pyodbc.Error as e:
        print('Error durante la consulta:', str(e))
        return jsonify({'message': 'Error en el servidor'}), 500

#########  CONSULTA:DEPENDENCIA  #########
@app.route('/consultar-dependencias', methods=['POST'])
def consultar_dependencias():
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        query = 'SELECT DE_DEPENDENCIA FROM [SGD_PCM].[IDOSGD].[RHTM_DEPENDENCIA]'
        cursor.execute(query)
        result = cursor.fetchall()

        dependencias = [record.DE_DEPENDENCIA for record in result]

        conn.close()

        return jsonify({'dependencias': dependencias})

    except pyodbc.Error as e:
        print('Error durante la consulta:', str(e))
        return jsonify({'message': 'Error en el servidor'}), 500

#########  CONSULTA-DEPENDENCIA-CODIGO  #########
@app.route('/consultar-dependencias-codigo', methods=['POST'])
def consultar_dependencias_codigo():
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        query = 'SELECT CO_DEPENDENCIA, DE_DEPENDENCIA FROM [SGD_PCM].[IDOSGD].[RHTM_DEPENDENCIA]'
        cursor.execute(query)
        result = cursor.fetchall()

        dependencias = [
            {
                'CO_DEPENDENCIA': record.CO_DEPENDENCIA,
                'DE_DEPENDENCIA': record.DE_DEPENDENCIA
            }
            for record in result
        ]

        conn.close()

        return jsonify({'dependencias': dependencias})

    except pyodbc.Error as e:
        print('Error durante la consulta:', str(e))
        return jsonify({'message': 'Error en el servidor'}), 500


#########  INSERTAR-MOVIMIENTO  #########
@app.route('/insertar-movimiento', methods=['POST'])
def insertar_movimiento():
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        # Obtener los datos del cuerpo de la solicitud
        request_data = request.get_json()
        items_to_insert = request_data.get('expedientes', [])
        estad_recep = 1  # Valor predeterminado para estadRecep

        for item in items_to_insert:
            NUM_EXPE = item.get('NUM_EXPE')
            NOM_DEP_EMI = item.get('NOM_DEP_EMI')
            DE_ASU = item.get('DE_ASU')
            NOM_DEP_REC = item.get('NOM_DEP_REC')
            REMI = item.get('REMI')
            FECH_DERI = item.get('FECH_DERI')
            EST_EXPE = item.get('EST_EXPE')

            # Insertar en MOVIM_EXPE
            cursor.execute(
                """
                INSERT INTO MOVIM_EXPE (NUM_EXPE, NOM_DEP_EMI, DE_ASU, NOM_DEP_REC, REMI, FECH_DERI, EST_EXPE)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                NUM_EXPE, NOM_DEP_EMI, DE_ASU, NOM_DEP_REC, REMI, FECH_DERI, EST_EXPE
            )

            # Insertar en EXPE_SGD_NEW_2
            cursor.execute(
                """
                INSERT INTO EXPE_SGD_NEW_2 (NU_EXPEDIENTE, DE_ASU, DEPEND_EMIS, DEPEND_RECEP, NOMBRE_REMITENTE, FE_EMI_CORTA, ESTADO)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                NUM_EXPE, DE_ASU, NOM_DEP_EMI, NOM_DEP_REC, REMI, FECH_DERI, estad_recep
            )

            # Actualizar el estado del registro superior 1
            cursor.execute(
                """
                UPDATE TOP (1) EXPE_SGD_NEW_2
                SET ESTADO = ?
                WHERE NU_EXPEDIENTE = ?;
                """,
                EST_EXPE, NUM_EXPE
            )

        conn.commit()
        conn.close()

        return jsonify({'message': 'Movimientos de expedientes insertados o actualizados correctamente.'}), 201

    except pyodbc.Error as e:
        print('Error al insertar o actualizar los movimientos de expedientes:', str(e))
        return jsonify({'message': 'Error en el servidor'}), 500
    
#########  CONSULTA-NUMERO-EXPEDIENTE  #########
@app.route('/consulta-expediente/<numeroExpediente>', methods=['GET'])
def consulta_expediente(numeroExpediente):
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        # Consulta SQL
        query = """
            SELECT NU_EXPEDIENTE, DEPEND_RECEP, TIPO_ESTA, FE_EMI_CORTA, OBSER
            FROM EXPE_SGD_NEW_2 AS EXPE
            INNER JOIN ESTA_DOC AS EST ON EST.ESTA_ID = EXPE.ESTADO
            WHERE EXPE.NU_EXPEDIENTE = ?
        """

        cursor.execute(query, numeroExpediente)
        result = cursor.fetchall()

        if len(result) == 0:
            return jsonify({'error': 'No se encontraron resultados para el expediente especificado.'}), 404

        expedientes = [
            {
                'NU_EXPEDIENTE': record.NU_EXPEDIENTE,
                'DEPEND_RECEP': record.DEPEND_RECEP,
                'TIPO_ESTA': record.TIPO_ESTA,
                'FE_EMI_CORTA': record.FE_EMI_CORTA,
                'OBSER': record.OBSER
            }
            for record in result
        ]

        conn.close()
        return jsonify(expedientes)

    except pyodbc.Error as e:
        print('Error al realizar la consulta del expediente:', str(e))
        return jsonify({'error': 'Hubo un error al consultar el expediente.'}), 500
    
@app.route('/Observacion-actualizar', methods=['POST'])
def actualizar_observacion():
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        data = request.json
        nu_expediente = data['nu_expediente']
        depend_emis = data['depend_emis']
        depend_recep = data['depend_recep']
        observacion = data['observacion']

        # Consulta SQL
        query = """
            UPDATE EXPE_SGD_NEW_2
            SET OBSER = ?, ESTADO = 3
            WHERE NU_EXPEDIENTE = ? AND DEPEND_EMIS = ? AND DEPEND_RECEP = ?
        """

        cursor.execute(query, observacion, nu_expediente, depend_emis, depend_recep)
        conn.commit()
        conn.close()

        print('Consulta SQL:', query)
        print('Expediente actualizado con éxito.')
        return 'Expediente actualizado con éxito.', 200

    except pyodbc.Error as e:
        print('Error al ejecutar la consulta SQL:', str(e))
        return 'Hubo un error al actualizar el expediente.', 500

if __name__ == '__main__':
    HOST = '0.0.0.0'
    PORT = int(os.environ.get('PORT', 5003))
    app.run(HOST, PORT)

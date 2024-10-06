#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <pthread.h>
#include <mysql.h>

int contador;

//Estructura necesaria para acceso excluyente
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int i;
int sockets[100];

void *AtenderCliente (void *socket)
{
	//Abrimos base de datos
	MYSQL *conn;
	MYSQL_RES *res;
	MYSQL_ROW row;
	int err;
	// Initialize the MySQL library
	mysql_library_init(0, NULL, NULL);
	
	if (mysql_library_init(0, NULL, NULL) != 0) {
		printf("Error initializing MySQL library: %u %s\n",
			   mysql_errno(NULL), mysql_error(NULL));
		exit(1);
	}
	
	//We create a connection to the MYSQL server
	conn = mysql_init(NULL);
	if (conn==NULL) {
		printf("Error creating connection: %u %s\n",
			   mysql_errno(conn), mysql_error(conn));
		exit (1);
	}
	//initialize the connection
	conn = mysql_real_connect (conn, "localhost","root", "mysql", "Uno",0, NULL, 0);
	if (conn==NULL) {
		printf("Error initializing connection: %u %s\n",
			   mysql_errno(conn), mysql_error(conn));
		exit (1);
	}
	
	int sock_conn;
	int *s;
	s= (int *) socket;
	sock_conn= *s;
	
	//int socket_conn = * (int *) socket;
	
	char peticion[512];
	char respuesta[512];
	int ret;
	
	
	int terminar =0;
	// Entramos en un bucle para atender todas las peticiones de este cliente
	//hasta que se desconecte
	while (terminar ==0)
	{
		// Ahora recibimos la petici?n
		ret=read(sock_conn,peticion, sizeof(peticion));
		printf ("Recibido\n");
		
		// Tenemos que a?adirle la marca de fin de string 
		// para que no escriba lo que hay despues en el buffer
		peticion[ret]='\0';
		
		
		printf ("Peticion: %s\n",peticion);
		
		// vamos a ver que quieren
		char *p = strtok( peticion, "/");
		int codigo =  atoi (p);
		int numForm;
		// Ya tenemos el c?digo de la petici?n
		char nombre[20];
		char query[80];
		
		if (codigo !=0)
		{
			p = strtok( NULL, "/");
			numForm =  atoi (p);
			p = strtok( NULL, "/");
			strcpy (nombre, p);
			// Ya tenemos el nombre
			printf ("Codigo: %d, Nombre: %s\n", codigo, nombre);
		}
		
		if (codigo ==0) //petici?n de desconexi?n
			terminar=1;
		else if (codigo ==1) //Partida con más movimientos de un jugador específico
			sprintf (query,"SELECT g.GameID, g.GameDate, g.NumOfMoves"
			"FROM Game g "
			"JOIN Turns t ON g.GameID = t.GameID "
			"JOIN Player p ON t.PlayerID = p.PlayerID "
			"WHERE p.Username = '%s' "
			"GROUP BY g.GameID "
			"ORDER BY g.NumOfMoves DESC "
			"LIMIT 1", nombre);
			sprintf (respuesta,"1/%d/%d",numForm,strlen (nombre));
			// Get the result
			res = mysql_store_result(conn);
			if (!res) {
				sprintf(respuesta,"Error getting result: %s\n", mysql_error(conn));
			}
			int printed = 0;
			// Print the result
			while ((row = mysql_fetch_row(res))) {
				if(!printed){
					sprintf(respuesta,"The game with most movements played by %s has the identifier %d ," 
						   "and it was held on the %s, with a total of %s moves.", username, atoi(row[0]), row[1], row[4]);
					printed = 1;
				}	
		
		else if (codigo ==2) //piden query 2
			
		else if (codigo ==3) //piden query 3
			
			if (codigo !=0)
			{
				
				printf ("Respuesta: %s\n", respuesta);
				// Enviamos respuesta
				write (sock_conn,respuesta, strlen(respuesta));
				
			}
			
	}
	// Se acabo el servicio para este cliente
	close(sock_conn); 
	
	// Free the result
	mysql_free_result(res);
	
	// Close the connection
	mysql_close(conn);
	
	// Deinitialize the MySQL library
	mysql_library_end();
}


int main(int argc, char *argv[])
{
	
	int sock_conn, sock_listen;
	struct sockaddr_in serv_adr;
	
	// INICIALITZACIONS
	// Obrim el socket
	if ((sock_listen = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		printf("Error creant socket");
	// Fem el bind al port
	
	
	memset(&serv_adr, 0, sizeof(serv_adr));// inicialitza a zero serv_addr
	serv_adr.sin_family = AF_INET;
	
	// asocia el socket a cualquiera de las IP de la m?quina. 
	//htonl formatea el numero que recibe al formato necesario
	serv_adr.sin_addr.s_addr = htonl(INADDR_ANY);
	// establecemos el puerto de escucha
	serv_adr.sin_port = htons(9050);
	if (bind(sock_listen, (struct sockaddr *) &serv_adr, sizeof(serv_adr)) < 0)
		printf ("Error al bind");
	
	if (listen(sock_listen, 3) < 0)
		printf("Error en el Listen");
	
	contador =0;
	
	pthread_t thread;
	i=0;
	for (;;){
		printf ("Escuchando\n");
		
		sock_conn = accept(sock_listen, NULL, NULL);
		printf ("He recibido conexion\n");
		
		sockets[i] =sock_conn;
		//sock_conn es el socket que usaremos para este cliente
		
		// Crear thead y decirle lo que tiene que hacer
		
		pthread_create (&thread, NULL, AtenderCliente,&sockets[i]);
		i=i+1;
		
	}
	

	
}
import logging
import azure.functions as func
import psycopg2
import psycopg2.extras
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def main(msg: func.ServiceBusMessage):
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    try:
        logging.info("Connecting to Postgres Server")
        connection = psycopg2.connect(
            host= os.getenv("POSTGRES_URL", ""),
            database= os.getenv("POSTGRES_DB", ""),
            user= os.getenv("POSTGRES_USER", ""),
            password= os.getenv("POSTGRES_PW", ""))
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # TODO: Get notification message and subject from database using the notification_id
        logging.info(f"Get notification {notification_id}")
        postgresql_query = "SELECT message, subject FROM notification where id = %s"
        cursor.execute(postgresql_query, (notification_id,))
        notification_db =  cursor.fetchone()

        # TODO: Get attendees email and name
        logging.info(f"Get all attendee")
        cursor.execute("SELECT email, first_name FROM attendee")
        attendees = cursor.fetchall()

        # TODO: Loop through each attendee and send an email with a personalized subject
        logging.info(f"Loop through each attendee to send an email")
        sendGrid = SendGridAPIClient(api_key=os.getenv("SENDGRID_API_KEY", ""))
        for attendee in attendees:
            message = Mail(
                from_email= os.getenv("ADMIN_EMAIL", "adminemail@gmail.com") ,
                to_emails= attendee["email"],
                subject= notification_db["subject"],
                html_content= f'Dear {attendee["first_name"]},<br><br>'
                              f'<strong>{notification_db["message"]}</strong>')
            response = sendGrid.send(message)
            logging.info(f"Send emai response {response.status_code} - {response.body} - {response.headers}")

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        logging.info(f"Update notification after sending an email, number of attendee: {len(attendees)}")
        cursor.execute("UPDATE notification SET completed_date = %s, status = %s WHERE id = %s",
                         (datetime.now(), f"Notified {len(attendees)} attendees", notification_id))
        connection.commit()
        logging.info(f"Send out emails and updated data to postgres database")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        connection.rollback()
    finally:
        # TODO: Close connection
        logging.info("Closing connection to Postgres Server")
        if connection:
            cursor.close()
            connection.close()
            logging.info("Postgres Server is closed connection")

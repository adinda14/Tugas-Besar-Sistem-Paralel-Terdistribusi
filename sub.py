# Tugas Besar Sistem Paralel dan Terdistribusi : LION AIR GROUP
# Kelas IF-43-10
# Anggota Kelompok :
# Arsenio Jusuf A (1301194043)
# Dita Julaika P (1301194244)
# Salma Salsabila F (1301194143)

# inisialisasi kamus
import paho.mqtt.client as mqtt # paho mqtt
import time # time for sleep()
import re # re untuk menemukan tanggal dan lokasi dalam string jadwal penerbangan

# inisialisasi port
port = 1883

# buat callback on_message; jika memungkinkan adanya pesan
# maka fungsi ini akan dipanggil secara asynch
########################################
list_tanggal = []
list_lokasi = []

# callback untuk mengolah data yang diterima dari subscribe topik
def on_message(client, userdata, msg):
  jadwal = str(msg.payload.decode("utf-8"))
  print(jadwal)

  # mencari tanggal dan jam pada jadwal penerbangan
  date = re.findall('\d{4}-\d{2}-\d{2}, \d{2}:\d{2}:\d{2}', jadwal)[0]

  # mencari lokasi pada jadwal penerbangan
  location = re.findall('Flight to (.+)', jadwal)[0]
  
  # menambahkan hasil pencarian dalam jadwal penerbangan kedalam list masing-masing
  list_tanggal.append(date)
  list_lokasi.append(location)
  
  # menuliskan isi list_lokasi dalam file lokasi.txt
  file1 = open('lokasi.txt','w')
  for x in list_lokasi:
    file1.write(x)
    file1.write('\n')
  file1.close

  # menuliskan isi list_tanggal dalam file boarding.txt
  file2 = open('boarding.txt','w')
  for y in list_tanggal:
    file2.write(y)
    file2.write('\n')
  file2.close
  
# callback untuk melakukan subscribe ketika terkoneksi
def on_connect(client, userdata, flags, rc):
    print("Connected! Result code: " + str(rc))
    client.subscribe("boarding")
########################################

# buat client baru bernama Subscriber
client = mqtt.Client("Subscriber")

# pengaitan fungsi callback ke client 
client.on_connect = on_connect
client.on_message = on_message

# mengkoneksikannya dengan broker
client.username_pw_set('lion','air')
client.connect('broker.emqx.io')
client.loop_forever()
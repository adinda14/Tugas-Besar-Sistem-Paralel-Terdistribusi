# Tugas Besar Sistem Paralel dan Terdistribusi : LION AIR GROUP
# Kelas IF-43-10
# Anggota Kelompok :
# Arsenio Jusuf A (1301194043)
# Dita Julaika P (1301194244)
# Salma Salsabila F (1301194143)

# inisialisasi kamus
import paho.mqtt.client as mqtt # paho mqtt
import time # time untuk sleep()
import datetime as dt # datetime untuk mendapatkan waktu dan tanggal
import random # random untuk generate angka secara acak

# inisialisasi port
port = 1883

# buat callback on_publish untuk publish data
########################################
def on_publish(client, userdata, mid):
    print("mid: " + str(mid))
########################################

# list tujuan
tujuan = ["Jakarta", "Bandung", "Bali", "Padang", "Balikpapan",
            "Pontianak", "Pekanbaru", "Palembang", "Medan", "Yogyakarta"]

# definisi nama broker yang akan digunakan
broker_address = "broker.emqx.io"
 
# membuat client baru bernama Publisher
print("Creating new instance")
client = mqtt.Client("Publisher") 

# mengaitkan callback on_publish ke client
client.on_publish = on_publish

# melakukan koneksi ke broker
print("connecting to broker")
client.username_pw_set('lion','air')
client.connect(broker_address)

# mulai loop client
client.loop_start()

# melakukan publish jadwal sebanyak range 11 kali
for i in range(11):
    # sleep 1 detik
    time.sleep(1)

    # menggunakan tanggal pada hari ini karena ingin mem-publish jadwal penerbangan hari ini
    tanggal = dt.date.today()

    # random waktu penerbangan (jam, menit, detik)
    jam = dt.time(random.randint(7,20), random.randrange(60), random.randrange(60))

    # membuat datetime baru bedasarkan tanggal dan waktu diatas
    datetime_baru = dt.datetime.combine(tanggal, jam)

    # memilih lokasi tujuan secara random dari list tujuan 
    lokasi_tujuan = random.randrange(0, len(tujuan))

    # memilih tempat transit secara random dari list tujuan
    tempat_transit = random.randrange(0, len(tujuan))

    # hanya mempublish apabila tujuan lokasi dan tempat transit tidak sama
    if (lokasi_tujuan != tempat_transit):
        jadwal = datetime_baru.strftime("%Y-%m-%d, %H:%M:%S") + " Flight " + "to " + tujuan[lokasi_tujuan] +" ," + "Transit : " + tujuan[tempat_transit]
        client.publish("boarding",jadwal)
        print(jadwal)
    else:
        i -= 1
    
#stop loop
client.loop_stop()
# network_intrusion_detection_system
## Description

Chương trình triển khai hệ thống giám sát và phát hiện các mối nguy hiểm được truyền thông qua kafka xem như đầu vào, sau đó được truyền đến Apache Spark Streaming để xử lý và phân loại đâu là dữ liệu bình thường (normal) và đâu là dữ liệu tấn công (attack) nhờ vào mô hình marchine learning. 

Usage:  $sudo ./run.sh
        $python ./sendTestByRate.py 100 ../Desktop/thangnt7ir/Datasets/NSL-KDD/KDDTest+_20Percent.txt | ../Desktop/kafka_2.12-3.3.1/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic netflows

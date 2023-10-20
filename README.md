# raizen_test
Desenvolvimento de código conforme teste solicitado para a vaga de Engenheira de Dados Sr da empresa Raízen

# Instalação

## pip

```bash
sudo apt install python3-pip
```

## Instalação e configuração do ambiente virtual

```bash
sudo pip3 install virtualenv

virtualenv airflow_env

source airflow_env/bin/activate
```

## Instalação do Airflow e bibliotecas necessárias
```bash
pip3 install apache-airflow[gcp,sentry,statsd]
```

## Criação da pasta "Dags", onde serão armazenadas as dags
```bash
mkdir dags
```

## Criação de usuário no Airflow
```bash
airflow users create --username admin --password your_password --firstname your_first_name --lastname your_last_name --role Admin --email your_email@domain.com

airflow users list
```

## Executando o Airflow
```bash
airflow scheduler
```

Após realizar os passos acima, abra uma aba no navegador e vá até o endereço http://localhost:8080/

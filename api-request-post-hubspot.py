import requests
import pandas as pd
import json

def verificar_e_postar_no_hubspot(access_token, endpoint, dados_para_postar):
    # configurando os cabeçalho e o token de acesso
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    # Verificar se os dados ja existem na API do HubSpot
    dados_existentes = verificar_dados_existentes(access_token, endpoint, dados_para_postar)

    if dados_existentes:
        print("Os dados já existem na API do HubSpot. Ignorando a postagem.")
        return
    else:
        # Se os dados nao existirem, prosseguir com a postagem
        resposta = postar_no_hubspot(access_token, endpoint, dados_para_postar, headers)
        if resposta.status_code == 200:
            print("Dados postados com sucesso.")
        else:
            print(f"Erro ao postar dados. Código de status: {resposta.status_code}")

def verificar_dados_existentes(access_token, endpoint, dados_para_postar):
    # Fazer uma solicitacao para verificar se os dados ja existem na API do HubSpot
    resposta = requests.get(endpoint, headers={'Authorization': f'Bearer {access_token}'})

    if resposta.status_code == 200:
        dados_existentes = resposta.json()
        # Verificar se dados_para_postar estao presentes em dados_existentes
        if dados_para_postar in dados_existentes:
            return True
    return False

def postar_no_hubspot(access_token, endpoint, dados_para_postar, headers):
    # Fazer uma solicitacao POST para postar dados na API do HubSpot
    resposta = requests.post(endpoint, json=dados_para_postar, headers=headers)
    return resposta

# Exemplo de uso:
access_token = 'hub-token'
hubspot_endpoint = 'https://api.hubapi.com/seu/endpoint/api'
dados_para_postar = {'chave1': 'valor1', 'chave2': 'valor2'}


# Chamando a funcao
verificar_e_postar_no_hubspot(access_token, hubspot_endpoint, dados_para_postar)
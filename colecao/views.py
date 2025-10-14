from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
import pandas as pd
from io import BytesIO
import logging
from .models import Colecao
import os
from django.db import transaction
from datetime import datetime
from django.core.paginator import Paginator
from django.shortcuts import redirect

logger = logging.getLogger(__name__)

@require_http_methods(["POST"])
def upload_colecao(request):
    if not request.FILES:
        return JsonResponse({'error': 'Nenhum arquivo enviado'}, status=400)

    arquivo = request.FILES['arquivo_excel']
    
    try:
        if not arquivo.name.endswith(('.xlsx', '.xls')):
            return JsonResponse({'error': 'Formato inválido. Use .xlsx ou .xls'}, status=415)

        ext = os.path.splitext(arquivo.name)[1].lower()
            
        with BytesIO() as buffer:
            for chunk in arquivo.chunks():
                buffer.write(chunk)
            buffer.seek(0)
            
            engine = None
            if ext == '.xlsx':
                engine = 'openpyxl'
            elif ext == '.xls':
                engine = 'xlrd'
            
            try:
                excel_reader = pd.read_excel(buffer, engine=engine)
            except Exception as e:
                logger.error(f"Erro ao ler Excel: {str(e)}")
                return JsonResponse({'error': 'Arquivo Excel corrompido ou inválido'}, status=400)

            print(f"Colunas encontradas: {list(excel_reader.columns)}")

            try:
                registros_criados = 0
                erros = []
                
                for _, linha in excel_reader.iterrows():    
                    try:
                        dados_linha = {
                            'numero_tombo': linha.get('Nº de Tombo', '').strip() if not pd.isna(linha.get('Nº de Tombo', '')) else '',
                            'reino': linha.get('Reino', '').strip() if not pd.isna(linha.get('Reino', '')) else '',
                            'filo': linha.get('Filo', '').strip() if not pd.isna(linha.get('Filo', '')) else '',
                            'classe': linha.get('Classe', '').strip() if not pd.isna(linha.get('Classe', '')) else '',
                            'ordem': linha.get('Ordem', '').strip() if not pd.isna(linha.get('Ordem', '')) else '',
                            'familia': linha.get('Família', '').strip() if not pd.isna(linha.get('Família', '')) else '',
                            'genero': linha.get('Gênero', '').strip() if not pd.isna(linha.get('Gênero', '')) else '',
                            'epiteto': linha.get('Epíteto', '').strip() if not pd.isna(linha.get('Epíteto', '')) else '',
                            'local': linha.get('Local', '').strip() if not pd.isna(linha.get('Local', '')) else '',
                            'continente': linha.get('Continente', '').strip() if not pd.isna(linha.get('Continente', '')) else '',
                            'municipio': linha.get('Município', '').strip() if not pd.isna(linha.get('Município', '')) else '',
                            'estado': linha.get('Estado', '').strip() if not pd.isna(linha.get('Estado', '')) else '',
                            'pais': linha.get('País', '').strip() if not pd.isna(linha.get('País', '')) else '',
                            'latitude': linha.get('Latitude', '').strip() if not pd.isna(linha.get('Latitude', '')) else '',
                            'longitude': linha.get('Longitude', '').strip() if not pd.isna(linha.get('Longitude', '')) else '',
                            'data': formatar_data_string(str(linha.get('Data', '')).strip()) if not pd.isna(linha.get('Data', '')) else '',
                            'coletores_1_last_name': linha.get('Coletores 1 Last Name', '').strip() if not pd.isna(linha.get('Coletores 1 Last Name', '')) else '',
                            'coletores_1_first_name': linha.get('Coletores 1 First Name', '').strip() if not pd.isna(linha.get('Coletores 1 First Name', '')) else '',
                            'coletores_2_last_name': linha.get('Coletores 2 Last Name', '').strip() if not pd.isna(linha.get('Coletores 2 Last Name', '')) else '',
                            'coletores_2_first_name': linha.get('Coletores 2 First Name', '').strip() if not pd.isna(linha.get('Coletores 2 First Name', '')) else '',
                            'coletores_3_last_name': linha.get('Coletores 3 Last Name', '').strip() if not pd.isna(linha.get('Coletores 3 Last Name', '')) else '',
                            'coletores_3_first_name': linha.get('Coletores 3 First Name', '').strip() if not pd.isna(linha.get('Coletores 3 First Name', '')) else '',
                            'coletores_4_last_name': linha.get('Coletores 4 Last Name', '').strip() if not pd.isna(linha.get('Coletores 4 Last Name', '')) else '',
                            'coletores_4_first_name': linha.get('Coletores 4 First Name', '').strip() if not pd.isna(linha.get('Coletores 4 First Name', '')) else '',
                            'coletores_5_last_name': linha.get('Coletores 5 Last Name', '').strip() if not pd.isna(linha.get('Coletores 5 Last Name', '')) else '',
                            'coletores_5_first_name': linha.get('Coletores 5 First Name', '').strip() if not pd.isna(linha.get('Coletores 5 First Name', '')) else '',
                            'coletores_6_last_name': linha.get('Coletores 6 Last Name', '').strip() if not pd.isna(linha.get('Coletores 6 Last Name', '')) else '',
                            'coletores_6_first_name': linha.get('Coletores 6 First Name', '').strip() if not pd.isna(linha.get('Coletores 6 First Name', '')) else '',
                            'coletores_7_last_name': linha.get('Coletores 7 Last Name', '').strip() if not pd.isna(linha.get('Coletores 7 Last Name', '')) else '',
                            'coletores_7_first_name': linha.get('Coletores 7 First Name', '').strip() if not pd.isna(linha.get('Coletores 7 First Name', '')) else '',
                            'determinador_1_last_name': linha.get('Determinador 1 Last Name', '').strip() if not pd.isna(linha.get('Determinador 1 Last Name', '')) else '',
                            'determinador_1_first_name': linha.get('Determinador 1 First Name', '').strip() if not pd.isna(linha.get('Determinador 1 First Name', '')) else '',
                            'determinador_2_last_name': linha.get('Determinador 2 Last Name', '').strip() if not pd.isna(linha.get('Determinador 2 Last Name', '')) else '',
                            'determinador_2_first_name': linha.get('Determinador 2 First Name', '').strip() if not pd.isna(linha.get('Determinador 2 First Name', '')) else '',
                            'determinador_3_last_name': linha.get('Determinador 3 Last Name', '').strip() if not pd.isna(linha.get('Determinador 3 Last Name', '')) else '',
                            'determinador_3_first_name': linha.get('Determinador 3 First Name', '').strip() if not pd.isna(linha.get('Determinador 3 First Name', '')) else '',
                            'determinador_4_last_name': linha.get('Determinador 4 Last Name', '').strip() if not pd.isna(linha.get('Determinador 4 Last Name', '')) else '',
                            'determinador_4_first_name': linha.get('Determinador 4 First Name', '').strip() if not pd.isna(linha.get('Determinador 4 First Name', '')) else '',
                            'determinador_5_last_name': linha.get('Determinador 5 Last Name', '').strip() if not pd.isna(linha.get('Determinador 5 Last Name', '')) else '',
                            'determinador_5_first_name': linha.get('Determinador 5 First Name', '').strip() if not pd.isna(linha.get('Determinador 5 First Name', '')) else '',
                            'determinador_6_last_name': linha.get('Determinador 6 Last Name', '').strip() if not pd.isna(linha.get('Determinador 6 Last Name', '')) else '',
                            'determinador_6_first_name': linha.get('Determinador 6 First Name', '').strip() if not pd.isna(linha.get('Determinador 6 First Name', '')) else '',
                            'determinador_7_last_name': linha.get('Determinador 7 Last Name', '').strip() if not pd.isna(linha.get('Determinador 7 Last Name', '')) else '',
                            'determinador_7_first_name': linha.get('Determinador 7 First Name', '').strip() if not pd.isna(linha.get('Determinador 7 First Name', '')) else '',
                            'curador_last_name': linha.get('Curador Last Name', '').strip() if not pd.isna(linha.get('Curador Last Name', '')) else '',
                            'curador_first_name': linha.get('Curador First Name', '').strip() if not pd.isna(linha.get('Curador First Name', '')) else '',
                            'observacao': linha.get('Observação', '').strip() if not pd.isna(linha.get('Observação', '')) else '',
                            'numero_total_exemplares': linha.get('Nº Total de Exemplares', 0) if not pd.isna(linha.get('Nº Total de Exemplares', 0)) else 0,
                            'numero_femeas_ovigeras': linha.get('Nº Fêmeas Ovígeras', 0) if not pd.isna(linha.get('Nº Fêmeas Ovígeras', 0)) else 0,
                            'numero_femeas': linha.get('Nº Fêmeas', 0) if not pd.isna(linha.get('Nº Fêmeas', 0)) else 0,
                            'numero_machos': linha.get('Nº Machos', 0) if not pd.isna(linha.get('Nº Machos', 0)) else 0,
                            'conservacao': linha.get('Conservação', '').strip() if not pd.isna(linha.get('Conservação', '')) else '',
                            'projeto': linha.get('Projeto', '').strip() if not pd.isna(linha.get('Projeto', '')) else '',
                            'status': linha.get('Status', '').strip() if not pd.isna(linha.get('Status', '')) else ''
                        }
                        
                        with transaction.atomic():
                                Colecao.objects.create(**dados_linha)
                                registros_criados += 1
                        
                    except Exception as e:
                        erros.append(f"Linha {_}: {str(e)}")
                        logger.warning(f"Erro na linha {_}: {str(e)}")
            
                logger.info(f"Importação concluída: {registros_criados} registros salvos, {len(erros)} erros")
                
                return redirect('visualiza_colecao') 
                    
            except Exception as e:
                logger.error(f"Erro na transação: {str(e)}")
                return JsonResponse({'error': f"Falha na importação: {str(e)}"}, status=500)

    except Exception as e:
        logger.exception("Erro durante processamento do Excel")
        return JsonResponse({'error': str(e)}, status=500)

def formatar_data_string(data_string):    
    try:
        return datetime.strptime(data_string, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')
    except ValueError:
        return data_string 

import json
from datetime import datetime

@require_http_methods(["GET"])
def visualiza_colecao(request):
    queryset = Colecao.objects.all()
    paginator = Paginator(queryset, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Converter QuerySet para formato JSON seguro para templates
    dados_json = json.dumps([{
        'Gênero': item.genero,
        'Espécie': item.epiteto,
        'Local de Coleta': item.local,
        'Data': item.data
    } for item in queryset])
    
    context = {
        'page_obj': page_obj,
        'dados_json': dados_json,  # Todos os dados para JS
        'dados_graficos': {
            'especies': preparar_dados_grafico_especies(queryset),
            'hierarquia': preparar_dados_hierarquia(queryset),
            'relacional': preparar_dados_relacional(queryset)
        }
    }
    return render(request, 'pages/colecao.html', context)

def preparar_dados_grafico_especies(queryset):
    # Preparar dados para o gráfico de espécies (top 20)
    contagem = {}
    for item in queryset:
        especie = item.epiteto or 'Desconhecida'
        contagem[especie] = contagem.get(especie, 0) + 1
    
    # Ordenar e pegar top 20
    top20 = sorted(contagem.items(), key=lambda x: x[1], reverse=True)[:20]
    return {
        'labels': [item[0] for item in top20],
        'values': [item[1] for item in top20]
    }

def preparar_dados_hierarquia(queryset):
    # Preparar dados para o gráfico hierárquico
    generos = {}
    for item in queryset:
        genero = item.genero or 'Desconhecido'
        especie = item.epiteto or 'Desconhecida'
        
        if genero not in generos:
            generos[genero] = set()
        generos[genero].add(especie)
    
    # Converter para estrutura de hierarquia
    hierarquia = {
        'name': 'Coleção',
        'children': [
            {
                'name': genero,
                'children': [{'name': especie} for especie in especies]
            } for genero, especies in generos.items()
        ]
    }
    return hierarquia

def preparar_dados_relacional(queryset):
    # Preparar dados para o grafo relacional (amostra de 500 itens)
    amostra = queryset[:500] if queryset.count() > 500 else queryset
    nodes = set()
    edges = []
    
    for item in amostra:
        especie = item.epiteto or 'Espécie Desconhecida'
        local = item.local or 'Local Desconhecido'
        
        nodes.add((1, especie, 'espécie'))
        nodes.add((2, local, 'local'))
        edges.append((especie, local))
    
    return {
        'nodes': [{'id': idx, 'label': label, 'group': group} 
                 for idx, (_, label, group) in enumerate(nodes)],
        'edges': [{'from': next(i for i, (_, l, _) in enumerate(nodes) if l == e),
                   'to': next(i for i, (_, l, _) in enumerate(nodes) if l == l)}
                  for e, l in edges]
        }
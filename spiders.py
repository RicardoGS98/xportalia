import datetime as dt
import json

from dateutil.relativedelta import relativedelta
from scrapy.http import HtmlResponse
from scrapy_scrapingbee import ScrapingBeeSpider, ScrapingBeeRequest

BEARER_TOKEN = "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ3ZWIyNDAwMzAyIiwiYXV0aCI6W10sImlkaW9tYSI6ImVzIiwiZW1haWwiOiJNY2FjZXJlc0B4cG9ydGFsaWEuY29tIiwiZW5hYmxlZCI6dHJ1ZSwibnVtZXJvQ2xpZW50ZSI6MjQwMDMsIm5vbWJyZUNsaWVudGUiOiJYcG9ydGFsaWEgLSBERU1PIiwicGFpc0NsaWVudGUiOiJVWSIsImRldk1vZGUiOmZhbHNlLCJmb3JtYXRvRXhjZWwiOiJ4bHN4IiwibWVudU9yaWdpbmFsIjp0cnVlLCJzaXN0ZW1hIjoieHBvcnRhbGlhIiwiaW5zdGFsYWRhIjpmYWxzZSwicmVzdHJpY2Npb25lcyI6W3siaWQiOjEsInRpcG8iOiJDYW50aWRhZEZpbGFzIiwib2JqZXRvQWZlY3RhZG8iOiJEZXRhbGxlLERlc2dsb3NlLERldGFsbGVSZWdpb25hbCxEZXRhbGxlQWN1bXVsYWRvcyxFbXByZXNhLEVtcHJlc2FEZXNnbG9zZSxGcmFuamFWYWxvcmVzLEhpc3Rvcmljb01lcmNhZGVyaWEsU2VjY2lvbixQYWlzLEJhbGFuemFDb21lcmNpYWwsUGFpc0VtcHJlc2EsUGFpc1J1YnJvLFNlY2Npb25Qb3NpY2lvbixTZWNjaW9uQ2FwaXR1bG8sU2VjY2lvbkVtcHJlc2EiLCJjYW1wb0FjY2lvbiI6ImZpbGFzIiwib3BlcmFkb3JBY2Npb24iOiI9IiwidmFsb3JBY2Npb24iOiI1IiwiY2FtcG9Db25kaWNpb24iOiIiLCJvcGVyYWRvckNvbmRpY2lvbiI6IiIsInZhbG9yZXNDb25kaWNpb24iOlsiIl19LHsiaWQiOjMsInRpcG8iOiJDYW1wb09jdWx0byIsIm9iamV0b0FmZWN0YWRvIjoiRGV0YWxsZSxEZXRhbGxlRmluYWwsSGlzdG9yaWNvTWVyY2FkZXJpYSxQYWlzRW1wcmVzYSxTZWNjaW9uRW1wcmVzYSxGcmFuamFWYWxvcmVzLEVzdHVkaW9NZXJjYWRvIiwiY2FtcG9BY2Npb24iOiJvcGVyYWRvckxvY2FsLG9wZXJhZG9yTG9jYWxJZCxvcGVyYWRvckxvY2FsRGlyZWNjaW9uLG9wZXJhZG9yRXh0cmFuamVybyxvcGVyYWRvckV4dHJhbmplcm9EaXJlY2Npb24sZGVzY3JpcGNpb24sb3BlcmFkb3JMb2NhbENvZGlnbyxvcGVyYWRvckV4dHJhbmplcm9Db2RpZ28iLCJvcGVyYWRvckFjY2lvbiI6Ij0iLCJ2YWxvckFjY2lvbiI6Ik5vIGRpc3BvbmlibGUiLCJjYW1wb0NvbmRpY2lvbiI6InBhaXMiLCJvcGVyYWRvckNvbmRpY2lvbiI6Ij0iLCJ2YWxvcmVzQ29uZGljaW9uIjpbIklOIl19LHsiaWQiOjE0LCJ0aXBvIjoiUGVyZmlsRGVzaGFiaWxpdGFkbyIsIm9iamV0b0FmZWN0YWRvIjoiUGVyZmlsIiwiY2FtcG9BY2Npb24iOiIiLCJvcGVyYWRvckFjY2lvbiI6IiIsInZhbG9yQWNjaW9uIjoiIiwiY2FtcG9Db25kaWNpb24iOiIiLCJvcGVyYWRvckNvbmRpY2lvbiI6IiIsInZhbG9yZXNDb25kaWNpb24iOlsiIl19LHsiaWQiOjMzLCJ0aXBvIjoiQ2FtcG9PY3VsdG8iLCJvYmpldG9BZmVjdGFkbyI6IkRldGFsbGUsRGV0YWxsZUZpbmFsIiwiY2FtcG9BY2Npb24iOiJvcGVyYWRvckxvY2FsLG9wZXJhZG9yTG9jYWxEaXJlY2Npb24sb3BlcmFkb3JFeHRyYW5qZXJvLG9wZXJhZG9yRXh0cmFuamVyb0RpcmVjY2lvbixudW1lcm9PcGVyYWNpb24sZGVjbGFyYW50ZUNvZGlnbyxkZWNsYXJhbnRlLG9wZXJhZG9yTG9jYWxDb2RpZ28sb3BlcmFkb3JFeHRyYW5qZXJvQ29kaWdvLG9wZXJhZG9yTG9jYWxJZCIsIm9wZXJhZG9yQWNjaW9uIjoiPSIsInZhbG9yQWNjaW9uIjoiTm8gZGlzcG9uaWJsZSIsImNhbXBvQ29uZGljaW9uIjoib3BlcmF0aXZhIiwib3BlcmFkb3JDb25kaWNpb24iOiI9IiwidmFsb3Jlc0NvbmRpY2lvbiI6WyJCT35pbXBvcnQiLCJCT35leHBvcnQiXX1dLCJ0aXBvVXN1YXJpbyI6IndlYiIsImlkIjoxMTc4NzM0LCJpYXQiOjE2NzM0NzYyNTMsImV4cCI6MTY3NDA4MTA1M30.nC_xtCURl3gGxsQXAOeWJO0x5HlAb0pcmi0OutGhfJU"
STANDARD_URL = 'https://xportalia.penta-transaction.com/PentaApi/detalle/{cod_pais}/{operacion}/parametros?version=6.4.0_7'
PAGINADO = 10
PAYLOAD = {
    "paginado": {
        "filaDesde": 0,
        "cantidad": PAGINADO
    }
}
LIST_PAIS = [
    # ARGENTINA
    {
        'operaciones': ['importDetalladas', 'exportDetalladas'],
        'periodo': ['2020-01-01', '2022-12-31'],
        'transportes': [
            {
                "clave": "8",
                "valor": "Acuática"
            },
            {
                "clave": "5",
                "valor": "Aéreo"
            },
            {
                "clave": "2",
                "valor": "Avión"
            },
            {
                "clave": "4",
                "valor": "Camión"
            },
            {
                "clave": "9",
                "valor": "Conductor Eléctrico"
            },
            {
                "clave": "3",
                "valor": "Ferrocarril"
            },
            {
                "clave": "6",
                "valor": "Jangada"
            },
            {
                "clave": "0",
                "valor": "No Determinado"
            },
            {
                "clave": "7",
                "valor": "Oleoducto-Gasoducto"
            },
            {
                "clave": "1",
                "valor": "Propios Medios"
            },
            {
                "clave": "A",
                "valor": "Vía Postal"
            }
        ],
        'payload': {
            'operativa': {
                "tipoConsulta": "cobol",
                "base": "/narald1/argentin",
                "paisCampos": "AR"
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'ayudas',
                        "tabla": 'PaisCobolArgentina'
                    }
                },
                {
                    "nombre": "transporte",
                    "valor": None
                }
            ]
        }
    },

    # BRAZIL
    # {
    #     'operacions': ['cargasMaritimasIngresos', 'cargasMaritimasSalidas'],
    #     'operativa': {
    #         "tipoConsulta": "mysql",
    #         "base": "brasil_cargas_maritimas",
    #         "paisCampos": "BR"
    #     },
    #     'parametros': [
    #         {
    #             "nombre": "periodo",
    #             "valor": '2020-01-01',
    #             "valor2": '2022-10-31'
    #         },
    #         {
    #             "nombre": "paisCodigo",
    #             "tipo": "autocompletado",
    #             "ayuda": {
    #                 "base": 'ayudas',
    #                 "tabla": 'PaisCobolArgentina'
    #             }
    #         },
    #         {
    #             "nombre": "transporte",
    #             "valor": {
    #                 "clave": "8",
    #                 "valor": "Acuática"
    #                 # Posibles valores:
    #                 # "clave": "8",
    #                 # "valor": "Acuática"
    #                 # "clave": "5",
    #                 # "valor": "Aéreo"
    #                 # "clave": "2",
    #                 # "valor": "Avión"
    #                 # "clave": "4",
    #                 # "valor": "Camión"
    #                 # "clave": "9",
    #                 # "valor": "Conductor Eléctrico"
    #                 # "clave": "3",
    #                 # "valor": "Ferrocarril"
    #                 # "clave": "6",
    #                 # "valor": "Jangada"
    #                 # "clave": "0",
    #                 # "valor": "No Determinado"
    #                 # "clave": "7",
    #                 # "valor": "Oleoducto-Gasoducto"
    #                 # "clave": "1",
    #                 # "valor": "Propios Medios"
    #                 # "clave": "A",
    #                 # "valor": "Vía Postal"
    #             },
    #         }
    #     ]
    # },

    # CHILE
    {
        'operaciones': ['import', 'export'],
        'periodo': ['2020-01-01', '2022-10-31'],
        'transportes': [
            {
                "clave": "04",
                "valor": "Aérea"
            },
            {
                "clave": "07",
                "valor": "Carretero-Terrestre"
            },
            {
                "clave": "06",
                "valor": "Ferroviario"
            },
            {
                "clave": "01",
                "valor": "Marítimo"
            },
            {
                "clave": "08",
                "valor": "Oleoductos"
            },
            {
                "clave": "10",
                "valor": "Otra"
            },
            {
                "clave": "05",
                "valor": "Postal"
            }
        ],
        'payload': {
            'operativa': {
                "tipoConsulta": "cobol",
                "base": "/narald1/chile",
                "paisCampos": "CL"
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'ayudas',
                        "tabla": 'PaisCobolChile'
                    }
                },
                {
                    "nombre": "transporte",
                    "valor": None
                }
            ]
        }
    },

    # COLOMBIA
    {
        'operaciones': ['import', 'export'],
        'periodo': ['2022-01-01', '2022-10-31'],
        'transportes': [
            {
                "clave": "4",
                "valor": "AEREA"
            },
            {
                "clave": "5",
                "valor": "CORREO"
            },
            {
                "clave": "8",
                "valor": "FLUVIAL/NAVEGAB"
            },
            {
                "clave": "7",
                "valor": "INSTAL FIJAS"
            },
            {
                "clave": "1",
                "valor": "MARITIMA"
            },
            {
                "clave": "3",
                "valor": "TERRESTRE"
            }
        ],
        'base': 'co_estadisticas_',
        'payload': {
            'operativa': {
                'tipoConsulta': 'elasticsearch',
                'base': None,
                'paisCampos': 'CO',
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'colombia_estadisticas',
                        "tabla": 'PaisISO'
                    }
                },
                {
                    'nombre': 'transporteCodigo',
                    'valor': None
                }
            ]
        }
    },

    # ECUADOR
    {
        'operaciones': ['import', 'export'],
        'periodo': ['2020-01-01', '2022-12-23'],
        'transportes': [
            {
                "clave": "1",
                "valor": "Aéreo"
            },
            {
                "clave": "2",
                "valor": "Marítimo"
            },
            {
                "clave": "3",
                "valor": "Terrestre"
            }
        ],
        'base': "ec_estadisticas_",
        'payload': {
            'operativa': {
                "tipoConsulta": "elasticsearch",
                "base": None,
                "paisCampos": "EC"
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'ecuador_estadisticas',
                        "tabla": 'PaisISO'
                    }
                },
                {
                    "nombre": "transporteCodigo",
                    "valor": None
                }
            ]
        }
    },

    # PARAGUAY
    {
        'operaciones': ['import', 'export'],
        'periodo': ['2020-01-01', '2022-11-30'],
        'transportes': [
            {
                "clave": "2",
                "valor": "ACUATICO"
            },
            {
                "clave": "3",
                "valor": "AVION"
            },
            {
                "clave": "1",
                "valor": "CAMION"
            },
            {
                "clave": "5",
                "valor": "FERROCARRIL"
            },
            {
                "clave": "4",
                "valor": "PROPIOS MEDIOS"
            }
        ],
        'base': 'py_estadisticas_',
        'payload': {
            'operativa': {
                "tipoConsulta": "elasticsearch",
                "base": None,
                "paisCampos": "PY"
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'paraguay_estadisticas',
                        "tabla": 'PaisISO'
                    }
                },
                {
                    "nombre": "transporteCodigo",
                    "valor": None
                }
            ]
        }
    },

    # PERU
    {
        'operaciones': ['import', 'export'],
        'periodo': ['2020-01-01', '2022-12-31'],
        'transportes': [
            {
                "clave": "04",
                "valor": "AEREA"
            },
            {
                "clave": "07",
                "valor": "CARRETERA"
            },
            {
                "clave": "05",
                "valor": "COURIER (ADUANA AEREA)"
            },
            {
                "clave": "06",
                "valor": "FERROVIARIA"
            },
            {
                "clave": "02",
                "valor": "FLUVIAL"
            },
            {
                "clave": "03",
                "valor": "LACUSTRE"
            },
            {
                "clave": "01",
                "valor": "MARITIMO"
            },
            {
                "clave": "09",
                "valor": "OTROS"
            },
            {
                "clave": "00",
                "valor": "POSTAL (ADUANA POSTAL)"
            },
            {
                "clave": "08",
                "valor": "TUBERIAS"
            }
        ],
        'pyaload': {
            'operativa': {
                "tipoConsulta": "cobol",
                "base": "/narald1/peru",
                "paisCampos": "PE"
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'ayudas',
                        "tabla": 'PaisCobolPeru'
                    }
                },
                {
                    "nombre": "transporte",
                    "valor": None
                }
            ]
        }
    },

    # URUGUAY
    {
        'operaciones': ['import', 'export'],
        'periodo': ['2020-01-01', '2023-01-10'],
        'transportes': [
            {
                "clave": "004",
                "valor": "Vía Aérea"
            },
            {
                "clave": "007",
                "valor": "Vía Carretera"
            },
            {
                "clave": "006",
                "valor": "Vía Ferroviaria"
            },
            {
                "clave": "002",
                "valor": "Vía Fluvial"
            },
            {
                "clave": "003",
                "valor": "Vía Lacustre"
            },
            {
                "clave": "001",
                "valor": "Vía Marítima"
            },
            {
                "clave": "005",
                "valor": "Vía Postal"
            },
            {
                "clave": "008",
                "valor": "Tuberías"
            },
            {
                "clave": "009",
                "valor": "Otras Vías"
            },
            {
                "clave": "000",
                "valor": "P.S.P.M"
            }
        ],
        'payload': {
            'operativa': {
                'operacion': None,
                "tipoConsulta": "cobol",
                "base": "/narald1/unix",
                "paisCampos": "UY"
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'ayudas',
                        "tabla": 'PaisCobolUruguay'
                    }
                },
                {
                    "nombre": "transporte",
                    "valor": None
                }
            ]
        }
    },

    # VENEZUELA
    {
        'operaciones': ['import', 'export'],
        'periodo': ['2020-01-01', '2021-12-31'],
        'transportes': [
            {
                "clave": "03",
                "valor": "AEREO"
            },
            {
                "clave": "07",
                "valor": "FLUVIAL"
            },
            {
                "clave": "02",
                "valor": "MARITIMO"
            },
            {
                "clave": "01",
                "valor": "TERRESTRE"
            }
        ],
        'payload': {
            'operativa': {
                "tipoConsulta": "cobol",
                "base": "/narald1/venezuela",
                "paisCampos": "VE"
            },
            'parametros': [
                {
                    "nombre": "periodo",
                    "valor": None,
                    "valor2": None
                },
                {
                    "nombre": "paisCodigo",
                    "tipo": "autocompletado",
                    "ayuda": {
                        "base": 'ayudas',
                        "tabla": 'PaisCobolVenezuela'
                    }
                },
                {
                    "nombre": "transporte",
                    "valor": None
                }
            ]
        }
    }
]

PARAMS = {
    'render_js': False,
}

HEADERS = {
    'Authorization': BEARER_TOKEN,
    'Content-Type': 'application/json; charset=UTF-8',
    'Cache-Control': 'no-cache',
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 '
    #               'Safari/537.36'
}


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def get_body_url():
    for pais in LIST_PAIS:
        # Extraemos las fechas finales a procesar y calculamos la cantidad de meses
        start, stop = pais['periodo']
        start = dt.date.fromisoformat(start)
        stop = dt.date.fromisoformat(stop)
        months = diff_month(stop, start)

        # Se extrae payload a modificar y los datos necesarios del pais comunes para todas las consultas
        payload = pais['payload']
        payload.update(**PAYLOAD)
        cod_pais = payload['operativa']['paisCampos']
        base = pais.get('base', None)
        for operacion in pais['operaciones']:

            # EL Url es general para la operacion
            url = STANDARD_URL.format(**{'operacion': operacion, 'cod_pais': cod_pais})

            # Las consultas que no se realizan a cobol varian sus bases segun la operacion
            operativa = payload['operativa']
            operativa['operacion'] = operacion
            if base:
                operativa['base'] = base + operacion

            # Se recorre por mes xq en casos como colombia no se pueden consultar mas de 60k filas
            for _ in range(1, months):
                if start > stop:
                    break
                valor2 = start + relativedelta(months=1)
                periodo = payload['parametros'][0]
                periodo['valor'], periodo['valor2'] = start.isoformat(), valor2.isoformat()
                start = valor2

                # Se filtra tambien por transporte
                for valor in pais['transportes']:
                    transporte = payload['parametros'][3]
                    transporte['valor'] = valor
                    yield json.dumps(payload), url


class XportaliaSpider(ScrapingBeeSpider):
    name = 'xportalia'

    def start_requests(self):
        for body, url in get_body_url():
            yield ScrapingBeeRequest(
                url=url,
                method='POST',
                params=PARAMS,
                headers=HEADERS,
                body=body,
                callback=self.parse
            )

    def parse(self, response: HtmlResponse, pagina=1, **kwargs):
        data = response.json()
        datos = data['datos']
        if not data['exito'] or not datos['filas']:
            # La consulta no devolvio datos
            return
        for fila in datos['filas']:
            yield fila

        if len(datos['filas']) == PAGINADO:
            fila_desde = pagina * PAGINADO
            payload = json.loads(response.request.body)
            payload['paginado']['fila_desde'] = fila_desde
            yield ScrapingBeeRequest(
                url=response.url,
                method='POST',
                params=PARAMS,
                headers=HEADERS,
                body=payload,
                callback=self.parse,
                cb_kwargs={'pagina': pagina + 1}
            )


if __name__ == '__main__':
    from formatters import PoliteLogFormatter
    from scrapy.crawler import CrawlerProcess

    IDE_SETTINGS = {
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
        'CONCURRENT_REQUESTS': 80,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 40,
        'AUTOTHROTTLE_ENABLED': False,

        # MiddleWares
        'SCRAPINGBEE_API_KEY': 'JHXJ0A1LLSIIFOFLCG7EUOEH0ROIFETJ8R7DEESV6Y8MYNYP6E61O1806RQ1KTBREEUD32CM7TIFGMR4',

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_scrapingbee.ScrapingBeeMiddleware': 725,
            'middlewares.RetryMiddleware': 750
        },

        # Logging Options:
        'FEEDS': {
            'data.jl': {
                'format': format,
                'overwrite': True
            }
        },
        'LOG_FILE': 'logging.log',
        'LOG_FORMATTER': PoliteLogFormatter,
        'LOG_ENABLED': True,
        'LOG_LEVEL': 'INFO',
        'LOGSTATS_INTERVAL': 60,
        'LOG_FILE_APPEND': False,

        # Retry Options:
        'DOWNLOAD_FAIL_ON_DATALOSS': False,

        # Cache Options:
        # 'HTTPCACHE_ENABLED': True,
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
    }

    process = CrawlerProcess(settings=IDE_SETTINGS)
    process.crawl(XportaliaSpider)
    process.start()

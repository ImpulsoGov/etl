
import logging.config
import logging


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p',
                    filename='src//impulsoetl//sisab//indicadores_municipios//registros.log', level=logging.INFO)
logger = logging.getLogger('ImpulsoGov | ETL de Indicadores')


ch = logging.StreamHandler()
ch.setLevel(logging.INFO) 


formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)


logger.addHandler(ch)
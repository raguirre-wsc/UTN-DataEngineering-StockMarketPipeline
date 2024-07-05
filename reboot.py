import DB_manager, API_manager
import os, json

#ESTE SCRYPT ES PARA REESTABLECER EL PROYECTO A UN ESTADO INICIAL EN CASO DE NECESITARLO

# #CREAMOS LOS DIRECTORIOS NUEVAMENTE
# project_root=os.path.join(os.path.dirname(__file__),"Datalake","Bronce")
# stocks_dir=os.path.join(project_root,"Stocks")
# market_dir=os.path.join(project_root,"Markets")
# state_json_dir=os.path.join(os.path.dirname(__file__),"state.json")

# if not os.path.exists(project_root):
#     os.makedirs(project_root)

# if not os.path.exists(stocks_dir):
#     os.makedirs(stocks_dir)

# if not os.path.exists(market_dir):
#     os.makedirs(market_dir)

#REESTABLECEMOS EL STATE.JSON
with open(os.path.join(os.path.dirname(__file__),"state.json"), 'w') as file:
    json.dump({}, file, indent=4)
API_manager.manager().reiniciar_fecha_busqueda()

#ELIMINAMOS LOS SCHEMAS DE LA BD
DB_manager.manager().delete_schemas()
from web3 import Web3
from web3.auto import w3
from web3.middleware import geth_poa_middleware

import traceback
import sys
from functools import lru_cache
from web3.contract import Contract
from web3._utils.events import get_event_data
from web3._utils.abi import exclude_indexed_event_inputs, get_abi_input_names, get_indexed_event_inputs, normalize_event_input_types
from web3.exceptions import MismatchedABI, LogTopicError
from web3.types import ABIEvent
from eth_utils import event_abi_to_log_topic, to_hex
from hexbytes import HexBytes

import json
import re
import csv
import os

import time

# Endereço do contrato da rota V2 da Uniswap
contract_address = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"

# ABI do contrato da rota V2 da Uniswap
contract_abi = '[{"inputs":[{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"amountADesired","type":"uint256"},{"internalType":"uint256","name":"amountBDesired","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountTokenDesired","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountIn","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountOut","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsIn","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"reserveA","type":"uint256"},{"internalType":"uint256","name":"reserveB","type":"uint256"}],"name":"quote","outputs":[{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETHSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermit","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermitSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityWithPermit","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapETHForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETHSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]'
pair_abi = '[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]'

accepted_functions = ['swapExactTokensForETHSupportingFeeOnTransferTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens', 'swapExactETHForTokens', 'swapExactTokensForETH', 'swapExactTokensForTokens']

# Inicializar a instância do Web3
w3 = Web3(Web3.HTTPProvider('https://eth.llamarpc.com'))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)  # Apenas se estiver usando um nó Parity Ethereum

# Criar objeto do contrato
contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=contract_abi)

@lru_cache(maxsize=None)
def _get_topic2abi(abi):
  if isinstance(abi, (str)):
    abi = json.loads(abi)

  event_abi = [a for a in abi if a['type'] == 'event']
  topic2abi = {event_abi_to_log_topic(_): _ for _ in event_abi}
  return topic2abi

@lru_cache(maxsize=None)
def _get_hex_topic(t):
  hex_t = HexBytes(t)
  return hex_t

def decode_tuple(t, target_field):
  output = dict()
  for i in range(len(t)):
    if isinstance(t[i], (bytes, bytearray)):
      output[target_field[i]['name']] = to_hex(t[i])
    elif isinstance(t[i], (tuple)):
      output[target_field[i]['name']] = decode_tuple(t[i], target_field[i]['components'])
    else:
      output[target_field[i]['name']] = t[i]
  return output

def decode_list_tuple(l, target_field):
  output = l
  for i in range(len(l)):
    output[i] = decode_tuple(l[i], target_field)
  return output

def decode_list(l):
  output = l
  for i in range(len(l)):
    if isinstance(l[i], (bytes, bytearray)):
      output[i] = to_hex(l[i])
    else:
      output[i] = l[i]
  return output

def convert_to_hex(arg, target_schema):
    """
    utility function to convert byte codes into human readable and json serializable data structures
    """
    output = dict()
    for k in arg:
        if isinstance(arg[k], (bytes, bytearray)):
            output[k] = to_hex(arg[k])
        elif isinstance(arg[k], (list)) and len(arg[k]) > 0:
            target = [a for a in target_schema if 'name' in a and a['name'] == k][0]
            if target['type'] == 'tuple[]':
                target_field = target['components']
                output[k] = decode_list_tuple(arg[k], target_field)
            else:
                output[k] = decode_list(arg[k])
        elif isinstance(arg[k], (tuple)):
            target_field = [a['components'] for a in target_schema if 'name' in a and a['name'] == k][0]
            output[k] = decode_tuple(arg[k], target_field)
        else:
            output[k] = arg[k]
    return output

@lru_cache(maxsize=None)
def _get_contract(address, abi):
  """
  This helps speed up execution of decoding across a large dataset by caching the contract object
  It assumes that we are decoding a small set, on the order of thousands, of target smart contracts
  """
  if isinstance(abi, (str)):
    abi = json.loads(abi)

  contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)
  return (contract, abi)

def decode_tx(address, input_data, abi):
  if abi is not None:
    try:
      (contract, abi) = _get_contract(address, abi)
      func_obj, func_params = contract.decode_function_input(input_data)
      target_schema = [a['inputs'] for a in abi if 'name' in a and a['name'] == func_obj.fn_name][0]
      decoded_func_params = convert_to_hex(func_params, target_schema)
      return (func_obj.fn_name, json.dumps(decoded_func_params), json.dumps(target_schema))
    except:
      e = sys.exc_info()[0]
      return ('decode error', repr(e), None)
  else:
    return ('no matching abi', None, None)

def decode_log(data, topics, abi):
  if abi is not None:

    topic2abi = _get_topic2abi(abi)

    log = {
      'address': None, #Web3.toChecksumAddress(address),
      'blockHash': None, #HexBytes(blockHash),
      'blockNumber': None,
      'data': data, 
      'logIndex': None,
      'topics': [_get_hex_topic(_) for _ in topics],
      'transactionHash': None, #HexBytes(transactionHash),
      'transactionIndex': None
    }
    event_abi = topic2abi[log['topics'][0]]
    evt_name = event_abi['name']

    data = get_event_data(w3.codec, event_abi, log)['args']
    target_schema = event_abi['inputs']
    decoded_data = convert_to_hex(data, target_schema)
    return (evt_name, json.dumps(decoded_data), json.dumps(target_schema))
    
  else:
    return ('no matching abi', None, None)



# Último bloco processado
last_block = w3.eth.block_number

while True:
    # Obter o bloco mais recente
    current_block = w3.eth.block_number

    print("Sucesso em obter o último bloco!")

    # Verificar se houve um novo bloco
    if current_block > last_block:
        # Iterar sobre os blocos entre o último bloco processado e o bloco mais recente
        for block_number in range(last_block + 1, current_block + 1):
            block = w3.eth.get_block(block_number)

            # Obtendo informações do bloco mais recente
            block_data = {
                "Block_ID": block.number,  # Identificador único do bloco
                "Número_Bloco": block.number,
                "Hash_Bloco": block.hash.hex(),
                "Timestamp_Bloco": block.timestamp,
                "Número_Transações": len(block.transactions)
            }

            block_filename = f"block_{block_number}.csv"
            block_fieldnames = list(block_data.keys())
            file_exists = os.path.isfile(block_filename)

            with open(block_filename, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=block_fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(block_data)

            file_name_swap = f"swap_{block_number}_transactions.csv"

            # Iterar sobre as transações do bloco
            for tx_hash in block['transactions']:
                tx = w3.eth.get_transaction(tx_hash)

                tx_data = {
                    "Block_ID": block.number,  # Chave estrangeira referenciando o bloco
                    "Hash_Transação": tx.hash.hex(),
                    "Remetente": tx["from"],
                    "Destinatário": tx["to"],
                    "Valor_(Wei)": tx.value,
                    "Taxa_Gás": tx.gasPrice,
                    "Limite_Gás": tx.gas,
                    "Status": "Confirmada" if tx.blockNumber is not None else "Pendente",
                    "Timestamp_Transação": w3.eth.get_block(tx.blockNumber).timestamp
                }

                # Verificar se a transação é para o contrato da rota V2 da Uniswap
                if tx['to'] == Web3.to_checksum_address(contract_address):

                    receipt = w3.eth.get_transaction_receipt(tx_hash)
                    token_transfers = receipt.get("logs", [])

                    first_ocorrence = False
                    amountIn = 0
                    amountOut = 0

                    print('Hash da transação: ', tx_hash.hex())

                    if receipt.status != 0:
                        for index, transfer in enumerate(token_transfers):
                            transfer_signature = transfer["topics"][0].hex()
                            if(transfer_signature == '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'):
                                if not first_ocorrence:
                                    data_transfer_swap = transfer["data"].hex()
                                    data_transfer_swap_decoded = decode_log(
                                    data_transfer_swap,
                                    [
                                    '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
                                    '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
                                    '0x000000000000000000000000242301fa62f0de9e3842a5fb4c0cdca67e3a2fab'],
                                    pair_abi)
                                    print(json.dumps(json.loads(data_transfer_swap_decoded[1]), indent=2))
                                    data_transfer_swap_decoded = json.loads(data_transfer_swap_decoded[1]) 
                                    first_ocorrence = True
                                    amountIn = data_transfer_swap_decoded["amount1In"] if data_transfer_swap_decoded["amount0In"] == 0 else data_transfer_swap_decoded["amount0In"]
                                if index == len(token_transfers) - 1 or index == len(token_transfers) - 2:
                                    data_transfer_swap = transfer["data"].hex()
                                    data_transfer_swap_decoded = decode_log(
                                    data_transfer_swap,
                                    [
                                    '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
                                    '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
                                    '0x000000000000000000000000242301fa62f0de9e3842a5fb4c0cdca67e3a2fab'],
                                    pair_abi)
                                    print(json.dumps(json.loads(data_transfer_swap_decoded[1]), indent=2))
                                    data_transfer_swap_decoded = json.loads(data_transfer_swap_decoded[1])
                                    amountOut = data_transfer_swap_decoded["amount1Out"] if data_transfer_swap_decoded["amount0Out"] == 0 else data_transfer_swap_decoded["amount0Out"]
                        # Decodificar o input da transação

                        decoded = decode_tx(tx['to'], tx['input'], contract_abi)
                        print("A Transação é de:", decoded[0])
                        print('Input decodificado:', decoded[1])
                        if decoded[0] in accepted_functions:

                            decoded_1 = json.loads(decoded[1])  # Convertendo a string em um dicionário

                            swap_data = {
                                "Block_ID": block_number,  # Identificador único do bloco
                                "type_Transaction": decoded[0],
                                "amountOutMin": decoded_1.get("amountOutMin"),
                                "amountOut": amountOut,
                                "amountIn": amountIn,
                                "fromTokenContract": decoded_1.get("path", [])[0],
                                "toTokenContract": decoded_1.get("path", [])[-1],
                                "hashTransaction": tx_hash.hex(),
                            }

                            swap_fieldnames = list(swap_data.keys())

                            file_exists = os.path.isfile(file_name_swap)

                            with open(file_name_swap, 'a', newline='') as csvfile:
                                writer = csv.DictWriter(csvfile, fieldnames=swap_fieldnames)
                                if not file_exists:
                                    writer.writeheader()
                                writer.writerow(swap_data)
                    else:
                        print("transacao cancelada")

                transactions_filename = f"transactions_{block_number}.csv"
                transactions_fieldnames = list(tx_data.keys())
                file_exists = os.path.isfile(transactions_filename)

                with open(transactions_filename, 'a', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=transactions_fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(tx_data)

        # Atualizar o último bloco processado
        last_block = current_block

    # Aguardar um tempo antes de verificar novamente
    time.sleep(5)
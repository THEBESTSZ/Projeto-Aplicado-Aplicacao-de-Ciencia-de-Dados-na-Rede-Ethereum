from web3 import Web3
from web3.auto import w3
from web3.middleware import geth_poa_middleware
from uniswap import Uniswap

import requests
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

import aiohttp
import asyncio
import aiofiles
import aiocsv
import re
from bs4 import BeautifulSoup

import json
import re
import csv
import os

import time

from dataclasses import dataclass

@dataclass
class TransferState:
    first_ocorrence: bool = False
    amountIn: int = 0
    amountOut: int = 0

# Endereço do contrato da rota V2 da Uniswap
contract_address = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"

# ABI do contrato da rota V2 da Uniswap
contract_abi = '[{"inputs":[{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"amountADesired","type":"uint256"},{"internalType":"uint256","name":"amountBDesired","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountTokenDesired","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountIn","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountOut","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsIn","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"reserveA","type":"uint256"},{"internalType":"uint256","name":"reserveB","type":"uint256"}],"name":"quote","outputs":[{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETHSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermit","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermitSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityWithPermit","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapETHForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETHSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]'
pair_abi = '[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]'

accepted_functions = ['swapExactTokensForETHSupportingFeeOnTransferTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens', 'swapExactETHForTokens', 'swapExactTokensForETH', 'swapExactTokensForTokens']
dai_contract = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
api_key_ethplorer = "freekey"

address_uniswap = None
private_key = None
version = 2
provider = 'https://eth.llamarpc.com'

uniswap = Uniswap(address=address_uniswap, private_key=private_key, version=version, provider=provider)
w3 = Web3(Web3.HTTPProvider(provider))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

last_block = w3.eth.block_number

async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

async def wait_for_new_block():
    current_block = w3.eth.block_number
    while current_block <= last_block:
        await asyncio.sleep(1)
        current_block = w3.eth.block_number
    return current_block

import aiofiles.os

async def write_csv(archive_name, data, fieldnames, folder):
    output_folder = "output"
    folder_path = os.path.join(output_folder, folder)
    file_path = os.path.join(output_folder, folder, archive_name)
    file_exists = os.path.isfile(file_path)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(folder_path, exist_ok=True)  # Criar a pasta "output" se ainda não existir

    file_mode = 'a' if file_exists else 'w'
    async with aiofiles.open(file_path, mode=file_mode, newline='', encoding='utf-8') as csvfile:
        writer = aiocsv.AsyncDictWriter(csvfile, fieldnames=fieldnames)
        if file_mode == 'w':
            await writer.writeheader()
        await writer.writerow(data)

async def getTokenSymbol(token_address):
    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=pair_abi)
    symbol = token_contract.functions.symbol().call()
    symbol = symbol.replace(" ", "_")
    return symbol

@lru_cache(maxsize=None)
def _get_topic2abi(abi):
    if isinstance(abi, str):
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
        elif isinstance(t[i], tuple):
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
    output = dict()
    for k in arg:
        if isinstance(arg[k], (bytes, bytearray)):
            output[k] = to_hex(arg[k])
        elif isinstance(arg[k], list) and len(arg[k]) > 0:
            target = [a for a in target_schema if 'name' in a and a['name'] == k][0]
            if target['type'] == 'tuple[]':
                target_field = target['components']
                output[k] = decode_list_tuple(arg[k], target_field)
            else:
                output[k] = decode_list(arg[k])
        elif isinstance(arg[k], tuple):
            target_field = [a['components'] for a in target_schema if 'name' in a and a['name'] == k][0]
            output[k] = decode_tuple(arg[k], target_field)
        else:
            output[k] = arg[k]
    return output

@lru_cache(maxsize=None)
def _get_contract(address, abi):
    if isinstance(abi, str):
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

async def decode_log(data, topics, abi):
    if abi is not None:
        topic2abi = _get_topic2abi(abi)
        log = {
            'address': None,
            'blockHash': None,
            'blockNumber': None,
            'data': data,
            'logIndex': None,
            'topics': [_get_hex_topic(_) for _ in topics],
            'transactionHash': None,
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

async def decode_transfer_event(transfer):
    data_transfer_swap = transfer["data"].hex()
    decoded_data = await decode_log(
        data_transfer_swap,
        [
            '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
            '0x000000000000000000000000242301fa62f0de9e3842a5fb4c0cdca67e3a2fab'
        ],
        pair_abi
    )
    decoded_data_json = json.loads(decoded_data[1])
    return decoded_data_json

async def process_transfer(state, transfer, index, len_token_transfers):
    transfer_signature = transfer["topics"][0].hex()
    if transfer_signature == '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822':
        if not state.first_ocorrence:
            decoded_data_json = await decode_transfer_event(transfer)
            print(json.dumps(decoded_data_json, indent=2))
            state.amountIn = decoded_data_json["amount1In"] if decoded_data_json["amount0In"] == 0 else decoded_data_json["amount0In"]
            state.first_ocorrence = True
        if index == len_token_transfers - 1 or index == len_token_transfers - 2:
            decoded_data_json = await decode_transfer_event(transfer)
            print(json.dumps(decoded_data_json, indent=2))
            state.amountOut = decoded_data_json["amount1Out"] if decoded_data_json["amount0Out"] == 0 else decoded_data_json["amount0Out"]

async def get_price_input_async(token_address, dai_address, token_amount):
    # Aguardar por um curto período de tempo para simular uma operação assíncrona
    await asyncio.sleep(0.1)

    # Retornar o resultado da chamada uniswap.get_price_input()
    return uniswap.get_price_input(token_address, dai_address, token_amount)

async def get_prices(fromTokenAddress, toTokenAddress):
    desired_output_amount = 1
    from_dai_needed = 'NaN'
    to_dai_needed = 'NaN'

    try:
        from_token_contract = w3.eth.contract(address=Web3.to_checksum_address(fromTokenAddress), abi=pair_abi)
        from_decimals = from_token_contract.functions.decimals().call()
        from_token_amount = desired_output_amount * 10**from_decimals
        to_token_contract = w3.eth.contract(address=Web3.to_checksum_address(toTokenAddress), abi=pair_abi)
        to_decimals = to_token_contract.functions.decimals().call()
        to_token_amount = desired_output_amount * 10**to_decimals

        # Executar as duas chamadas em paralelo e aguardar a conclusão de ambas
        from_dai_needed, to_dai_needed = await asyncio.gather(
            get_price_input_async(Web3.to_checksum_address(fromTokenAddress), dai_contract, from_token_amount),
            get_price_input_async(Web3.to_checksum_address(toTokenAddress), dai_contract, to_token_amount)
        )

        # Converter os resultados para o formato desejado
        from_dai_needed = from_dai_needed / 10**18
        to_dai_needed = to_dai_needed / 10**18
    except Exception as e:
        print(f"Erro ao obter preços: {str(e)}")

    return from_dai_needed, to_dai_needed

async def get_holders_web_scrapping(fromTokenAddress, toTokenAddress):
    from_token_page_url = f'https://etherscan.io/token/{fromTokenAddress}'
    to_token_page_url = f'https://etherscan.io/token/{toTokenAddress}'

    from_holders_count = 'NaN'
    to_holders_count = 'NaN'

    try:
        # Executar as duas chamadas em paralelo e aguardar a conclusão de ambas
        html_from, html_to = await asyncio.gather(
            fetch(from_token_page_url),
            fetch(to_token_page_url)
        )

        soup_from = BeautifulSoup(html_from, 'html.parser')
        soup_to = BeautifulSoup(html_to, 'html.parser')

        holders_div_from = soup_from.find('div', id='ContentPlaceHolder1_tr_tokenHolders')
        holders_div_to = soup_to.find('div', id='ContentPlaceHolder1_tr_tokenHolders')

    except Exception as e:
        print(f"Erro durante a requisição: {str(e)}")

    try:
        html = await fetch(from_token_page_url)
        soup = BeautifulSoup(html, 'html.parser')
        holders_div = soup.find('div', id='ContentPlaceHolder1_tr_tokenHolders')

        if holders_div:
            holders_info_div = holders_div.find('div', class_='d-flex flex-wrap gap-2')

            if holders_info_div:
                holders_text = holders_info_div.get_text(strip=True)
                from_holders_count = re.sub(r'\(.*\)', '', holders_text).replace(',', '')
                print(f"O número atual de holders do token é: {from_holders_count.strip()}")
            else:
                print("Div com a classe 'd-flex flex-wrap gap-2' não encontrada dentro da div de holders")
        else:
            print("Div de holders não encontrada no HTML da página")

    except Exception as e:
        print(f"Erro durante a requisição: {str(e)}")

    try:
        html = await fetch(to_token_page_url)
        soup = BeautifulSoup(html, 'html.parser')
        holders_div = soup.find('div', id='ContentPlaceHolder1_tr_tokenHolders')

        if holders_div:
            holders_info_div = holders_div.find('div', class_='d-flex flex-wrap gap-2')

            if holders_info_div:
                holders_text = holders_info_div.get_text(strip=True)
                to_holders_count = re.sub(r'\(.*\)', '', holders_text).replace(',', '')
                print(f"O número atual de holders do token é: {to_holders_count.strip()}")
            else:
                print("Div com a classe 'd-flex flex-wrap gap-2' não encontrada dentro da div de holders")
        else:
            print("Div de holders não encontrada no HTML da página")
    except Exception as e:
        print(f"Erro durante a requisição: {str(e)}")

    return from_holders_count, to_holders_count

async def process_block(block):
    block_data = {
        "Block_ID": block.number,
        "Hash_Block": block.hash.hex(),
        "Miner": block.miner,
        "Difficulty": block.difficulty,
        "Total_Difficulty": block.totalDifficulty,
        "Size": block.size,
        "Gas_Limit": block.gasLimit,
        "Gas_Used": block.gasUsed,
        "Timestamp_Block": block.timestamp,
        "Number_Transactions": len(block.transactions)
    }

    await write_csv(f"block.csv", block_data, block_data.keys(), 'block')

    for tx_hash in block.transactions:
        tx = w3.eth.get_transaction(tx_hash)
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        tx_data = {
            "Block_ID": block.number,
            "Hash_Transaction": tx.hash.hex(),
            "From": tx["from"],
            "To": tx["to"],
            "Value_(Wei)": tx.value,
            "Gas_Price": tx.gasPrice,
            "Gas_Limit": tx.gas,
            "Gas_Used": receipt.gasUsed,
            "Status": "Confirmed" if tx.blockNumber is not None else "Pending",
            "Timestamp_Transaction": block.timestamp
        }

        if tx['to'] == Web3.to_checksum_address(contract_address):
            token_transfers = receipt.get("logs", [])

            state = TransferState()

            print('Hash da transação: ', tx_hash.hex())

            if receipt.status != 0:
                for index, transfer in enumerate(token_transfers):
                    await process_transfer(state, transfer, index, len(token_transfers))

                decoded = decode_tx(tx['to'], tx['input'], contract_abi)
                print("A Transação é de:", decoded[0])
                print('Input decodificado:', decoded[1])
                if decoded[0] in accepted_functions:
                    decoded_1 = json.loads(decoded[1])
                    fromTokenAddress = decoded_1.get('path', [])[0]
                    toTokenAddress = decoded_1.get('path', [])[-1]

                    holders_count_from, holders_count_to = await get_holders_web_scrapping(fromTokenAddress, toTokenAddress)
                    from_dai_needed, to_dai_needed = await get_prices(fromTokenAddress, toTokenAddress)
                    from_token_symbol = await getTokenSymbol(fromTokenAddress)
                    to_token_symbol = await getTokenSymbol(toTokenAddress)

                    swap_data = {
                        "Block_ID": block.number,
                        "Type_Transaction": decoded[0],
                        "Amount_Out_Min": decoded_1.get("amountOutMin"),
                        "Amount_Out": state.amountOut,
                        "Amount_In": state.amountIn,
                        "From_Token_Address": fromTokenAddress,
                        "To_Token_Address": toTokenAddress,
                        "From_Token_Symbol": from_token_symbol,
                        "To_Token_Symbol": to_token_symbol,
                        "From_Token_Holders_Count": holders_count_from,
                        "To_Token_Holders_Count": holders_count_to,
                        "From_Token_Price": from_dai_needed,
                        "To_Token_Price": to_dai_needed,
                        "Hash_Transaction": tx_hash.hex(),
                    }

                    await write_csv(f"swap_transactions.csv", swap_data, swap_data.keys(), 'swap')
            else:
                print("Transação cancelada")

        await write_csv(f"transactions.csv", tx_data, list(tx_data.keys()), 'transaction')

async def main():
    global last_block

    while True:
        new_block = await wait_for_new_block()
        print("Sucesso em obter o último bloco!")

        blocks = [w3.eth.get_block(block_number) for block_number in range(last_block + 1, new_block + 1)]
        await asyncio.gather(*[process_block(block) for block in blocks])

        last_block = new_block

asyncio.run(main())
def from_hex(hex_data: str):
    return int(hex_data, 16)


class Transaction:
    block = False
    user_from = False
    user_to = False
    gas_offered = False
    gas_used = False
    gas_price = False
    tx_hash = False
    nonce = False
    value = False

    def __init__(self, data):
        self.block = from_hex(data['blockNumber'])
        self.user_from = data['from']
        self.user_to = data['to']
        self.gas_offered = from_hex(data['gas'])
        self.gas_price = from_hex(data['gasPrice'])
        self.tx_hash = data['hash']
        self.nonce = from_hex(data['nonce'])
        self.value = from_hex(data['value'])

    def get_value(self) -> str:
        return str(self.value)

    def get_gas_offered(self) -> str:
        return str(self.gas_offered)

    def get_gas_used(self) -> str:
        return str(self.gas_used)

    def get_hash(self) -> str:
        if str(self.tx_hash)[0:2] == '0x':
            return str(self.tx_hash)[2:]
        return str(self.tx_hash)

    def check_transaction(self) -> bool:
        if self.value is False:
            raise Exception("Transacao nao preenchida value")
        if self.gas_used is False:
            raise Exception("Transacao nao preenchida gas used")
        if self.gas_offered is False:
            raise Exception("Transacao nao preenchida gas offered")

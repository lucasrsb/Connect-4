import json
import time
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock

class Connect4Game:
    def __init__(self, player_id):
        self.player_id = player_id
        self.zk = KazooClient(hosts='localhost:2181')
        self.zk.start()
        self.initialize_znodes()
        self.board = self.load_board()
        self.turn_lock = Lock(self.zk, '/connect4/turn_lock')
        self.PLAYER_SYMBOLS = {
          'player1': '1',  
          'player2': '2'   
        }

    def initialize_znodes(self):
        base_znodes = {
            '/connect4': b"",
            '/connect4/players': b"",
            '/connect4/board': b'[[null,null,null,null,null,null,null],'
                              b'[null,null,null,null,null,null,null],'
                              b'[null,null,null,null,null,null,null],'
                              b'[null,null,null,null,null,null,null],'
                              b'[null,null,null,null,null,null,null],'
                              b'[null,null,null,null,null,null,null]]',
            '/connect4/turn': b"player1",
            '/connect4/lock': b"",
            '/connect4/turn_lock': b""
        }
        for path, data in base_znodes.items():
            if not self.zk.exists(path):
                self.zk.create(path, data, makepath=True)
            elif path == '/connect4/board':
                current_data, _ = self.zk.get(path)
                if current_data == b"" or current_data == b"[]":
                    self.zk.set(path, data)

    def load_board(self):
        data, _ = self.zk.get('/connect4/board')
        try:
            loaded = json.loads(data.decode())
            return [[None if cell is None or cell == 'null' else cell for cell in row] for row in loaded]
        except (json.JSONDecodeError, AttributeError):
            return [[None for _ in range(7)] for _ in range(6)]

    def save_board(self):
        board_data = json.dumps(self.board).encode()
        self.zk.set('/connect4/board', board_data)

    def wait_for_opponent(self):
        print("Aguardando outro jogador...")
        while len(self.zk.get_children('/connect4/players')) < 2:
            time.sleep(1)
        print("Jogador conectado! Iniciando jogo.")

    def play(self):
        player_path = f"/connect4/players/{self.player_id}"
        self.zk.create(player_path, ephemeral=True)
        self.wait_for_opponent()

        @self.zk.DataWatch('/connect4/board')
        def watch_board(data, stat):
            if data:
                self.board = self.load_board()
                self.print_board()

        @self.zk.DataWatch('/connect4/turn')
        def watch_turn(data, stat):
            if data and data.decode() == self.player_id:
                self.handle_turn()

        while True:
            try:
                current_turn, _ = self.zk.get('/connect4/turn')
                if current_turn == 'r':
                  self.reset_board()
                  break
                if current_turn.decode() == self.player_id:
                    self.handle_turn()
                time.sleep(1)
            except KeyboardInterrupt:
                print("\nSaindo do jogo...")
                break

    def handle_turn(self):
        with self.turn_lock:  
            current_turn, _ = self.zk.get('/connect4/turn')
            if current_turn.decode() == self.player_id:
                self.make_move()

    def make_move(self):
        lock = Lock(self.zk, '/connect4/lock')
        with lock:
            while True:
                try:
                    col = int(input(f"{self.player_id}, escolha uma coluna (0-6): "))
                    if 0 <= col < 7:
                        break
                    print("Coluna inválida! Escolha entre 0 e 6.")
                except ValueError:
                    print("Entrada inválida! Digite um número entre 0 e 6.")
            
            for row in range(5, -1, -1):
                if self.board[row][col] is None:
                    self.board[row][col] = self.player_id
                    self.save_board()
                    next_player = "player2" if self.player_id == "player1" else "player1"
                    self.zk.set("/connect4/turn", next_player.encode())
                    if self.check_winner(row, col):
                        print(f"{self.player_id} venceu!")
                        self.zk.set("/connect4/winner", self.player_id.encode())
                        self.print_board()
                        self.zk.stop()
                        self.zk.close()
                        exit()
                    return
            print("Coluna cheia! Escolha outra coluna.")

    def check_winner(self, row, col):
        return False  

    def print_board(self):
        print("\n 0 1 2 3 4 5 6")
        print("+" + "-"*13 + "+")
        for row in self.board:
            print("|" + " ".join(
              " " if cell is None else self.PLAYER_SYMBOLS.get(cell)
              for cell in row) + "|")
        print("+" + "-"*13 + "+")

if __name__ == "__main__":
    player_id = input("Digite seu ID (player1 ou player2): ")
    game = Connect4Game(player_id)
    try:
        game.play()
    except KeyboardInterrupt:
        print("\nJogo encerrado pelo usuário.")
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        game.zk.stop()
        game.zk.close()


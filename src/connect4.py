import json
import time
import threading
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock

class Connect4Game:
    def __init__(self, player_id):
        self.player_id = player_id
        self.zk = KazooClient(hosts='localhost:2181')
        self.zk.start()        
        self.my_turn_event = threading.Event()
        self.game_over_event = threading.Event()        
        self.initialize_znodes()
        self.board = self.load_board()
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
            '/connect4/winner': b""
        }
        for path, data in base_znodes.items():
            if not self.zk.exists(path):
                lock = Lock(self.zk, '/connect4/init_lock')
                with lock:
                    if not self.zk.exists(path):
                        self.zk.create(path, data, makepath=True)

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
        if not self.zk.exists(player_path):
            self.zk.create(player_path, ephemeral=True)
        self.wait_for_opponent()
        self.print_board()

        @self.zk.DataWatch('/connect4/board')
        def watch_board(data, stat):
            if data and not self.game_over_event.is_set():
                print("\nTabuleiro atualizado:")
                self.board = self.load_board()
                self.print_board()

        @self.zk.DataWatch('/connect4/turn')
        def watch_turn(data, stat):
            if data and data.decode() == self.player_id:
                self.my_turn_event.set()

        @self.zk.DataWatch('/connect4/winner')
        def watch_winner(data, stat):
            if data:
                self.game_over_event.set()

        current_turn, _ = self.zk.get('/connect4/turn')
        if current_turn.decode() == self.player_id:
            self.my_turn_event.set() 
        while not self.game_over_event.is_set():
            turn_signaled = self.my_turn_event.wait(timeout=1.0)
            
            if self.game_over_event.is_set():
                break

            if turn_signaled:
                self.my_turn_event.clear()
                self.make_move()
        
        winner_data, _ = self.zk.get('/connect4/winner')
        self.board = self.load_board()
        self.print_board()
        print("\n=====================")
        print(f"FIM DE JOGO!")
        print(f"O vencedor é: {winner_data.decode()}")
        print("=====================")

    def make_move(self):
        lock = Lock(self.zk, '/connect4/move_lock')
        with lock:
            current_turn, _ = self.zk.get('/connect4/turn')
            if current_turn.decode() != self.player_id:
                return

            while True:
                try:
                    col = int(input(f"É a sua vez, {self.player_id}. Escolha uma coluna (0-6): "))
                    if 0 <= col < 7 and self.board[0][col] is None:
                        break
                    else:
                        print("Coluna inválida ou cheia. Tente novamente.")
                except ValueError:
                    print("Entrada inválida! Digite um número.")
            
            for row in range(5, -1, -1):
                if self.board[row][col] is None:
                    self.board[row][col] = self.player_id
                    self.save_board()
                    if self.check_winner(row, col):
                        self.zk.set("/connect4/winner", self.player_id.encode())
                        return
                    
                    next_player = "player2" if self.player_id == "player1" else "player1"
                    self.zk.set("/connect4/turn", next_player.encode())
                    return

    def check_winner(self, row, col):
        player = self.player_id
        for r in range(6):
            for c in range(4):
                if (self.board[r][c] == player and self.board[r][c+1] == player and
                    self.board[r][c+2] == player and self.board[r][c+3] == player):
                    return True
        for c in range(7):
            for r in range(3):
                if (self.board[r][c] == player and self.board[r+1][c] == player and
                    self.board[r+2][c] == player and self.board[r+3][c] == player):
                    return True
        for r in range(3):
            for c in range(4):
                if (self.board[r][c] == player and self.board[r+1][c+1] == player and
                    self.board[r+2][c+2] == player and self.board[r+3][c+3] == player):
                    return True
        for r in range(3, 6):
            for c in range(4):
                if (self.board[r][c] == player and self.board[r-1][c+1] == player and
                    self.board[r-2][c+2] == player and self.board[r-3][c+3] == player):
                    return True
        return False

    def print_board(self):
        print("\n  0 1 2 3 4 5 6")
        print(" +" + "-"*13 + "+")
        for row in self.board:
            print(" |" + " ".join(" " if cell is None else self.PLAYER_SYMBOLS.get(cell, '?') for cell in row) + "|")
        print(" +" + "-"*13 + "+")
        
    def close(self):
        self.zk.stop()
        self.zk.close()

if __name__ == "__main__":
    game = None
    try:
        player_id = input("Digite seu ID (player1 ou player2): ")
        if player_id not in ['player1', 'player2']:
            raise ValueError("ID de jogador inválido. Use 'player1' ou 'player2'.")
        game = Connect4Game(player_id)
        game.play()
    except KeyboardInterrupt:
        print("\nJogo encerrado pelo usuário.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        if game:
            game.close()

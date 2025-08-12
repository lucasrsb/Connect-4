"""
Implementação do jogo 'Connect 4' usando ZooKeeper para coordenação entre jogadores.
Desenvolvido para a disciplina de Sistemas Distribuídos.

Autores:
- Davi Nunes de Paiva - 11202231636
- Bruno Zujenas Ribeiro - 11202230963
- Lucas Roberto Santos  - 11202231370

-------------------------------------------------------------------------------------
Implementação dos conceitos solicitados:
-------------------------------------------------------------------------------------

Barriers: Uma barreira foi usada na função `iniciar()` para sincronizar os
jogadores, garantindo que o jogo só comece quando todos estiverem conectados.

Queues: A troca de turnos funciona como uma fila lógica, onde cada jogador aguarda sua
vez de jogar ser sinalizada pelo ZooKeeper antes de realizar uma ação.

Locks: O `Lock` distribuído foi aplicado na função `iniciar()` para garantir 
que a operação de reset do jogo seja executada por apenas um jogador por vez.

Leader Election: O primeiro jogador é definido através de uma `Election` na função 
`iniciar()`, onde um líder é eleito de forma distribuída para realizar a primeira jogada.
"""

import json
import time
import threading
import sys
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
from kazoo.recipe.election import Election
from kazoo.exceptions import NodeExistsError

class JogoConnect4:
    def __init__(self, id_jogador, modo='jogador'):
        self.meu_id = id_jogador
        self.modo = modo
        
        #Configuração da conexão com o ZooKeeper
        print(f"Conectando ao ZooKeeper como {self.meu_id}...")
        self.zk = KazooClient(hosts='127.0.0.1:2181', timeout=15)
        self.zk.start()
        
        #Variáveis para controlar o estado do jogo
        self.jogo_acabou = threading.Event()
        self.minha_vez = threading.Event() if modo == 'jogador' else None
        
        #Inicialização dos timers
        self.timer_turno = None
        self.timer_conexao = None

        if self.modo == 'jogador':
            self._criar_estrutura_base()
        
        self.tabuleiro = self._carregar_tabuleiro_remoto()

    def _criar_estrutura_base(self):
        """Garante que os nós (znodes) principais existem no ZooKeeper."""
        self.zk.ensure_path('/connect4/jogadores')
        self.zk.ensure_path('/connect4/vez')
        self.zk.ensure_path('/connect4/vencedor')
        self.zk.ensure_path('/connect4/motivo_vitoria')
        self.zk.ensure_path('/connect4/eleicao')
        
        if not self.zk.exists('/connect4/tabuleiro'):
            tabuleiro_vazio = json.dumps([[None]*7 for _ in range(6)]).encode()
            self.zk.create('/connect4/tabuleiro', tabuleiro_vazio)

    def _limpar_para_novo_jogo(self):
        """Limpa o estado do jogo anterior no ZooKeeper."""
        print("Limpando o servidor para um novo jogo...")
        self.zk.set("/connect4/vencedor", b"")
        self.zk.set("/connect4/vez", b"")
        self.zk.set("/connect4/motivo_vitoria", b"")
        
        tabuleiro_vazio = json.dumps([[None]*7 for _ in range(6)]).encode()
        self.zk.set("/connect4/tabuleiro", tabuleiro_vazio)
        
        #Remove jogadores que possam ter ficado da partida anterior
        jogadores_path = "/connect4/jogadores"
        if self.zk.exists(jogadores_path):
            for jogador in self.zk.get_children(jogadores_path):
                self.zk.delete(f"{jogadores_path}/{jogador}", recursive=True)

    def _carregar_tabuleiro_remoto(self):
        """Pega o estado atual do tabuleiro do ZooKeeper."""
        try:
            dados, _ = self.zk.get('/connect4/tabuleiro')
            return json.loads(dados.decode())
        except Exception:
            return [[None]*7 for _ in range(6)]

    def _salvar_tabuleiro_remoto(self):
        """Manda o estado atual do tabuleiro para o ZooKeeper."""
        dados_tabuleiro = json.dumps(self.tabuleiro).encode()
        self.zk.set('/connect4/tabuleiro', dados_tabuleiro)

    def _iniciar_observadores(self):
        """Registra as funções que vão reagir a mudanças no ZooKeeper."""
        self.zk.DataWatch('/connect4/vez')(self._observar_turno)
        self.zk.DataWatch('/connect4/vencedor')(self._observar_vencedor)
        self.zk.ChildrenWatch('/connect4/jogadores')(self._observar_jogadores)
    
    def _observar_jogadores(self, jogadores):
        """Chamado quando um jogador entra ou sai."""
        if self.jogo_acabou.is_set(): return False
        
        timer_rodando = self.timer_conexao and self.timer_conexao.is_alive()

        if len(jogadores) < 2 and not timer_rodando:
            if self.timer_turno and self.timer_turno.is_alive():
                self.timer_turno.cancel()

            print("\nOponente desconectou! Iniciando timer de 30s para W.O...")
            self.timer_conexao = threading.Timer(30.0, self._vencer_por_wo, args=['desconexao'])
            self.timer_conexao.start()
        elif len(jogadores) >= 2 and timer_rodando:
            print("\nOponente voltou!")
            self.timer_conexao.cancel()

    def _observar_turno(self, dados, stat):
        """Chamado quando o turno muda."""
        if self.jogo_acabou.is_set() or not dados: return

        if self.timer_turno and self.timer_turno.is_alive():
            self.timer_turno.cancel()

        jogador_da_vez = dados.decode()
        if jogador_da_vez == self.meu_id:
            print("\n>> É a sua vez!")
            self.minha_vez.set()
        else:
            self.minha_vez.clear()
            self.timer_turno = threading.Timer(30.0, self._vencer_por_wo, args=['tempo'])
            self.timer_turno.start()

    def _observar_vencedor(self, dados, stat):
        """Chamado quando um vencedor é declarado, finalizando o jogo."""
        if dados and not self.jogo_acabou.is_set():
            self.jogo_acabou.set()
            
            if self.timer_turno: self.timer_turno.cancel()
            if self.timer_conexao: self.timer_conexao.cancel()

    def _vencer_por_wo(self, motivo):
        """Função chamada pelos timers para declarar vitória."""
        
        if not self.zk.get("/connect4/vencedor")[0]:
            print(f"\nFim de jogo! Vitória para {self.meu_id} por {motivo}.")
            self.zk.set('/connect4/motivo_vitoria', motivo.encode())
            self.zk.set('/connect4/vencedor', self.meu_id.encode())

    def iniciar(self):
        """Lógica principal para o modo jogador."""
        
        with Lock(self.zk, "/connect4/lock_reinicio"):
            jogadores_ativos = self.zk.get_children("/connect4/jogadores")
            if not jogadores_ativos:
                self._limpar_para_novo_jogo()
                self.tabuleiro = self._carregar_tabuleiro_remoto()
        
        try:
            self.zk.create(f"/connect4/jogadores/{self.meu_id}", ephemeral=True)
        except NodeExistsError:
            print(f"ERRO: O ID '{self.meu_id}' já está em uso! Tente outro.")
            return

        print("Aguardando oponente...")
        while len(self.zk.get_children('/connect4/jogadores')) < 2:
            if self.jogo_acabou.is_set(): return
            time.sleep(1)
        print("Oponente conectado. O jogo vai começar!")

        if not self.zk.get('/connect4/vez')[0]:
            print("Sorteando quem vai ser o primeiro...")
            eleicao = Election(self.zk, '/connect4/eleicao', identifier=self.meu_id)
            eleicao.run(self._definir_primeiro_jogador)
        
        self._iniciar_observadores()
        print("\n=== VALENDO! ===")

        while not self.jogo_acabou.is_set():
            if self.minha_vez.wait(timeout=1.0):
                self._fazer_minha_jogada()
                self.minha_vez.clear()
        
        self._mostrar_resultado()
    
    def _definir_primeiro_jogador(self):
        """Callback chamado pela eleição quando um líder é escolhido."""
        print(f"Eu, {self.meu_id}, começo jogando!")
        self.zk.set('/connect4/vez', self.meu_id.encode())

    def _fazer_minha_jogada(self):
        """Processa a jogada do usuário, desde o input até salvar no ZK."""
        self.tabuleiro = self._carregar_tabuleiro_remoto()
        self._mostrar_tabuleiro()

        while True:
            coluna_escolhida = [None] 

            def _thread_de_input():
                try:
                    raw_input = input(f"Sua jogada, {self.meu_id}. Coluna (0-6): ")
                    coluna_escolhida[0] = int(raw_input)
                except (ValueError, EOFError):
                    pass
            
            input_thread = threading.Thread(target=_thread_de_input, daemon=True)
            input_thread.start()
            
            while input_thread.is_alive():
                if self.jogo_acabou.is_set():
                    print("\nO jogo terminou enquanto você se preparava para jogar.")
                    return
                time.sleep(0.1)

            coluna = coluna_escolhida[0]

            if coluna is None or not 0 <= coluna <= 6:
                print("Coluna inválida. Tente um número de 0 a 6.")
                continue
            
            if self.tabuleiro[0][coluna] is not None:
                print("Essa coluna já está cheia. Tente outra.")
                continue
            
            break

        for linha in range(5, -1, -1):
            if self.tabuleiro[linha][coluna] is None:
                self.tabuleiro[linha][coluna] = self.meu_id
                self._salvar_tabuleiro_remoto()
                
                if self._verificar_se_ganhei():
                    print("Boa! Você conectou 4 peças!")
                    self.zk.set("/connect4/motivo_vitoria", b"vitoria")
                    self.zk.set("/connect4/vencedor", self.meu_id.encode())
                else:
                    proximo = "player2" if self.meu_id == "player1" else "player1"
                    self.zk.set("/connect4/vez", proximo.encode())
                return

    def _verificar_se_ganhei(self):
        """Verifica todas as condições de vitória para o jogador atual."""
        j = self.meu_id
        b = self.tabuleiro
        for r in range(6):
            for c in range(4):
                if all(b[r][c+i] == j for i in range(4)): return True
        for c in range(7):
            for r in range(3):
                if all(b[r+i][c] == j for i in range(4)): return True
        for r in range(3):
            for c in range(4):
                if all(b[r+i][c+i] == j for i in range(4)): return True
        for r in range(3, 6):
            for c in range(4):
                if all(b[r-i][c+i] == j for i in range(4)): return True
        return False

    def _mostrar_tabuleiro(self):
        """Exibe o tabuleiro no console."""
        print("\n" + "-"*25)
        for r in range(6):
            linha_str = " ".join('X' if cell == 'player1' else 'O' if cell == 'player2' else '.' for cell in self.tabuleiro[r])
            print(f"| {linha_str} |")
        print("  0 1 2 3 4 5 6  ")
        print("-" * 25)

    def _mostrar_resultado(self):
        """Exibe a tela final de jogo."""
        try:
            vencedor = self.zk.get('/connect4/vencedor')[0].decode()
            motivo = self.zk.get('/connect4/motivo_vitoria')[0].decode()
        except Exception as e:
            print(f"Não foi possível obter o resultado final: {e}")
            return

        self.tabuleiro = self._carregar_tabuleiro_remoto()
        self._mostrar_tabuleiro()

        print("\n\n=== FIM DE JOGO ===")

        perdedor = 'player1' if vencedor == 'player2' else 'player2'

        if self.modo == 'espectador':
            print(f"Vencedor da partida: {vencedor}")
            if motivo == 'vitoria':
                print("Motivo: Conectou 4 peças em linha.")
            elif motivo == 'tempo':
                print(f"Motivo: O jogador {perdedor} excedeu o tempo de jogada.")
            elif motivo == 'desconexao':
                print(f"Motivo: O jogador {perdedor} desconectou.")

        else:
            if vencedor == self.meu_id:
                print("PARABÉNS, VOCÊ VENCEU!")
                if motivo == 'vitoria':
                    print("Motivo: Você conectou 4 peças em linha.")
                elif motivo == 'tempo':
                    print(f"Motivo: O oponente ({perdedor}) excedeu o tempo de jogada.")
                elif motivo == 'desconexao':
                    print(f"Motivo: O oponente ({perdedor}) desconectou.")
            else:
                print(f"Que pena, você perdeu. O vencedor foi {vencedor}.")
                if motivo == 'vitoria':
                    print(f"Motivo: {vencedor} conectou 4 peças em linha.")
                elif motivo == 'tempo':
                    print("Motivo: Você excedeu o seu tempo de jogada.")
                elif motivo == 'desconexao':
                    print("Motivo: Você foi considerado desconectado da partida.")
        print("===================\n")

    def assistir(self):
        """Lógica para o modo espectador."""
        print("Entrando em modo espectador. Aguardando o jogo começar...")
        self._mostrar_tabuleiro()

        @self.zk.DataWatch('/connect4/tabuleiro')
        def _observar_tabuleiro_spec(dados, stat):
            if not self.jogo_acabou.is_set():
                print("\n(Espectador) O tabuleiro mudou:")
                self.tabuleiro = self._carregar_tabuleiro_remoto()
                self._mostrar_tabuleiro()

        self.zk.DataWatch('/connect4/vencedor')(self._observar_vencedor)
        
        self.jogo_acabou.wait()
        self._mostrar_resultado()

    def fechar(self):
        """Limpa a conexão com o ZooKeeper."""
        print("Encerrando conexão...")
        if self.zk and self.zk.connected:
            self.zk.stop()
            self.zk.close()

if __name__ == "__main__":
    jogo = None
    try:
        print("--- Bem-vindo ao Connect 4 Distribuído ---")
        meu_id = input("Digite seu ID (player1, player2 ou espectador): ").strip().lower()
        
        if meu_id in ['player1', 'player2']:
            jogo = JogoConnect4(meu_id, modo='jogador')
            jogo.iniciar()
        elif meu_id == 'espectador':
            jogo = JogoConnect4('espectador', modo='espectador')
            jogo.assistir()
        else:
            print("ID inválido! Encerrando.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nSaindo do jogo.")
    except Exception as e:
        print(f"\nOcorreu um erro crítico: {e}")
    finally:
        if jogo:
            jogo.fechar()

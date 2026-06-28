"""
Intrigues & Couronne — jeu de société politique en ligne, multijoueur,
temps réel. Application Flask + Flask-SocketIO en un seul fichier.

LANCEMENT LOCAL
    pip install -r requirements.txt
    python3 app.py
    -> http://localhost:5000  (SQLite local automatique : fichier game.db)

DÉPLOIEMENT RAILWAY
    1. Poussez ce dossier sur un repo Git, créez un projet Railway dessus.
    2. Ajoutez un service PostgreSQL (Railway injecte DATABASE_URL tout seul,
       ce fichier bascule automatiquement de SQLite vers Postgres si cette
       variable est présente).
    3. Dans les Settings du service web, ajoutez ces variables d'environnement
       (Railway utilise Railpack comme builder par défaut, qui les lit) :
         RAILPACK_PYTHON_VERSION = 3.12
         RAILPACK_START_CMD      = gunicorn -k eventlet -w 1 --timeout 120 app:app
         SECRET_KEY              = (une valeur aléatoire)
    4. Déployez : Railway fournit une URL publique (*.up.railway.app).

   Un seul worker gunicorn (-w 1) est nécessaire : l'état des parties vit en
   mémoire (en plus d'être sauvegardé en DB à chaque coup), donc plusieurs
   workers verraient des états différents pour un même salon.
"""

from __future__ import annotations

import os
import json
import random
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, join_room, emit

# ============================================================================
# 1. DÉFINITIONS DES RÔLES
# ============================================================================


@dataclass(frozen=True)
class RoleDef:
    id: str
    name: str
    icon: str
    color: str
    flux: str
    voie_serviteur: str
    voie_fourbe: str
    ouvrir_desc: str
    fermer_desc: str
    statut_ministere: int  # poids dans Puissance = ... + (Statut × Stabilité)


ROLES: dict[str, RoleDef] = {
    "interieur": RoleDef(
        id="interieur",
        name="Ministre de l'Intérieur",
        icon="📜",
        color="#6b8fd4",
        flux="L'Information et les Rapports de Crise. Seul à piocher et lire "
             "le contenu réel des cartes d'événements avant de les remonter au Roi.",
        voie_serviteur="Communiquer fidèlement les crises, étouffer les rumeurs "
                        "et stabiliser le peuple en échange de gratifications royales.",
        voie_fourbe="Falsifier les rapports, revendre des informations secrètes ou "
                     "accepter des pots-de-vin pour embellir le bilan d'un autre ministre.",
        ouvrir_desc="Transmet des rapports clairs : la crise piochée est annoncée "
                     "avec ses véritables effets.",
        fermer_desc="Ment sur la crise : peut modifier les montants annoncés avant "
                     "résolution, au risque qu'elle empire si démasquée.",
        statut_ministere=2,
    ),
    "finances": RoleDef(
        id="finances",
        name="Surintendant des Finances",
        icon="🏛️",
        color="#d4b76b",
        flux="L'Or Public et les Investissements. Valide et distribue les fonds "
             "nécessaires aux actions des autres ministères.",
        voie_serviteur="Faire fructifier les caisses par des investissements sains, "
                        "déclenchant des Primes de Performance étatiques.",
        voie_fourbe="Créer des lignes budgétaires fictives, prélever des taxes "
                     "abusives non déclarées pour remplir sa cassette personnelle.",
        ouvrir_desc="Finance généreusement : +Or public, petite chance de Prime "
                     "(+Or personnel).",
        fermer_desc="Bloque les budgets : +Or personnel (détourné), mais risque "
                     "de faire chuter les Caisses de l'État.",
        statut_ministere=3,
    ),
    "aumonier": RoleDef(
        id="aumonier",
        name="Grand Aumônier",
        icon="⛪",
        color="#a98fd4",
        flux="La Légitimité et la Ferveur Populaire. Contrôle l'absolution "
             "morale et l'humeur spirituelle des masses.",
        voie_serviteur="Bénir les décrets, calmer les grognes paysannes par la "
                        "foi pour maintenir la Stabilité générale.",
        voie_fourbe="Accuser ses rivaux d'hérésie pour bloquer leurs décrets, et "
                     "monnayer de lourdes indulgences en or personnel.",
        ouvrir_desc="Accorde sa caution morale : +Stabilité du Royaume.",
        fermer_desc="Lance un interdit religieux : +Or personnel (indulgences), "
                     "mais risque de faire chuter la Stabilité.",
        statut_ministere=2,
    ),
    "subsistances": RoleDef(
        id="subsistances",
        name="Grand Maître des Subsistances",
        icon="🌾",
        color="#7bbf6a",
        flux="Le Ravitaillement et la Logistique. Gère les greniers à blé et "
             "les convois indispensables à tout mouvement d'envergure.",
        voie_serviteur="Assurer la distribution de nourriture dans les "
                        "provinces, générant des rentes commerciales stables.",
        voie_fourbe="Stocker et spéculer sur le grain en période de disette "
                     "pour faire exploser les prix au marché noir.",
        ouvrir_desc="Nourrit le Royaume : +Or public et +Stabilité légère.",
        fermer_desc="Spécule sur le grain : +Or personnel, mais risque de "
                     "Stabilité en baisse (famine).",
        statut_ministere=1,
    ),
    "connetable": RoleDef(
        id="connetable",
        name="Connétable",
        icon="🛡️",
        color="#c46a6a",
        flux="La Maréchaussée et l'Exécution des Ordres. Commande la garde "
             "royale et assure la sécurité physique à la cour.",
        voie_serviteur="Protéger les ministres des complots extérieurs et "
                        "exécuter promptement les décrets d'arrestation du Roi.",
        voie_fourbe="Fermer les yeux sur les tentatives de vol ou d'assassinat "
                     "entre ministres en échange d'une commission.",
        ouvrir_desc="Maintient l'ordre : protège les Caisses et l'Or personnel "
                     "de tous contre le vol ce cycle.",
        fermer_desc="Laisse planer l'anarchie : +Or personnel (commissions), "
                     "mais expose tout le monde aux vols/pillages.",
        statut_ministere=1,
    ),
}

ROLE_ORDER = ["interieur", "finances", "aumonier", "subsistances", "connetable"]

KING_ROLE = {
    "id": "roi",
    "name": "Le Roi",
    "icon": "👑",
    "color": "#f0c419",
    "description": (
        "Arbitre suprême de la partie. Le Roi ne possède pas de robinet : il "
        "reçoit les rapports du Ministre de l'Intérieur, peut révoquer un "
        "ministre suspecté de trahison, et valide la bonne tenue du Royaume. "
        "Il ne compte pas dans le calcul de Puissance Politique."
    ),
}

STABILITE_MAX = 100
STABILITE_INITIALE = 70
OR_PUBLIC_INITIAL = 100
NB_CYCLES = 5
MIN_TOTAL_PLAYERS = 3   # Roi + 2 ministres
MAX_TOTAL_PLAYERS = 6   # Roi + 5 ministres


def nb_ministres_for(total_players: int) -> int:
    """
    Nombre de ministres actifs (hors Roi) pour un effectif total donné.

    Règle : Roi + 2 ministres minimum (table à 3). Au-delà de 4 joueurs au
    total, on réserve volontairement un joueur "libre" (sans portefeuille)
    quand c'est possible, pour qu'il puisse servir de successeur dédié en
    cas de démission ou de révocation sans devoir cumuler deux postes.
    """
    non_king = total_players - 1
    if non_king <= 2:
        return max(2, non_king)
    if non_king == 3:
        return 3
    return min(5, non_king - 1)


# ============================================================================
# 2. DECK DE CARTES DE CRISE
# ============================================================================


@dataclass(frozen=True)
class CrisisCard:
    id: str
    titre: str
    texte: str
    stabilite_delta: int
    or_public_delta: int


CRISIS_DECK: list[CrisisCard] = [
    CrisisCard("c01", "Récolte exceptionnelle", "Les greniers débordent dans trois provinces.", 4, 15),
    CrisisCard("c02", "Épidémie de fièvre des marais", "La maladie se propage dans les bas quartiers.", -10, -12),
    CrisisCard("c03", "Banditisme sur la route royale", "Une caravane marchande est attaquée.", -3, -18),
    CrisisCard("c04", "Mariage princier annoncé", "La cour exulte, le peuple festoie.", 8, -10),
    CrisisCard("c05", "Incendie aux entrepôts du port", "Une partie des réserves d'huile a brûlé.", -5, -20),
    CrisisCard("c06", "Pèlerinage massif", "Des foules convergent vers la cathédrale.", 5, 5),
    CrisisCard("c07", "Sécheresse dans le Sud", "Les puits commencent à s'assécher.", -6, -8),
    CrisisCard("c08", "Découverte d'un gisement d'argent", "Une mine prometteuse est révélée.", 2, 25),
    CrisisCard("c09", "Émeute de la faim à la capitale", "La foule réclame du pain devant le palais.", -15, -5),
    CrisisCard("c10", "Visite d'un ambassadeur étranger", "Cadeaux diplomatiques et faste de circonstance.", 3, -15),
    CrisisCard("c11", "Naufrage de la flotte d'impôts", "Une partie des taxes maritimes est perdue en mer.", -2, -22),
    CrisisCard("c12", "Foire commerciale fructueuse", "Les marchands affluent de tout le royaume.", 3, 18),
    CrisisCard("c13", "Rumeurs de complot à la cour", "Des murmures de trahison circulent.", -8, 0),
    CrisisCard("c14", "Don du clergé aux pauvres", "Une distribution de vivres calme les tensions.", 7, -6),
    CrisisCard("c15", "Inondation des terres basses", "Les champs sont submergés après l'orage.", -7, -10),
    CrisisCard("c16", "Tournoi de chevalerie", "Le peuple acclame ses champions.", 6, -8),
    CrisisCard("c17", "Corruption découverte aux douanes", "Un scandale éclabousse l'administration.", -9, -14),
    CrisisCard("c18", "Alliance commerciale signée", "Un traité avantageux est ratifié.", 4, 20),
    CrisisCard("c19", "Peste du bétail", "Les troupeaux dépérissent dans les campagnes.", -8, -15),
    CrisisCard("c20", "Fête des moissons", "Une célébration unanime traverse le royaume.", 9, 6),
    CrisisCard("c21", "Désertion dans la garnison", "Des soldats impayés quittent leur poste.", -6, -9),
    CrisisCard("c22", "Legs d'un noble mourant", "Une fortune inattendue rejoint les caisses.", 1, 30),
    CrisisCard("c23", "Hiver précoce et rude", "Le froid frappe plus tôt que prévu.", -5, -12),
    CrisisCard("c24", "Réconciliation de deux maisons rivales", "La paix nobiliaire rassure la cour.", 6, 4),
    CrisisCard("c25", "Effondrement d'un pont royal", "Des travaux d'urgence s'imposent.", -4, -25),
]

CARD_BY_ID = {c.id: c for c in CRISIS_DECK}


def build_shuffled_deck(seed: int | None = None) -> list[str]:
    ids = [c.id for c in CRISIS_DECK]
    random.Random(seed).shuffle(ids)
    return ids


# ============================================================================
# 3. MOTEUR DE JEU
# ============================================================================


class Phase(str, Enum):
    LOBBY = "lobby"
    DISCUSSION = "discussion"
    DECISION = "decision"
    RAPPORT = "rapport"
    RESOLUTION = "resolution"
    TERMINEE_VICTOIRE = "terminee_victoire"
    TERMINEE_RUINE = "terminee_ruine"


@dataclass
class PlayerState:
    sid: str
    player_uid: str
    pseudo: str
    role_ids: list[str] = field(default_factory=list)  # cumul possible
    is_king: bool = False
    or_personnel: int = 0
    influence: int = 0
    connected: bool = True
    resigned_roles: list[str] = field(default_factory=list)

    def to_public_dict(self) -> dict:
        return asdict(self)

    @property
    def role_id(self) -> str | None:
        return self.role_ids[0] if self.role_ids else None

    @property
    def has_resigned(self) -> bool:
        return len(self.resigned_roles) > 0


@dataclass
class GameState:
    room_code: str
    phase: Phase = Phase.LOBBY
    cycle: int = 1
    stabilite: int = STABILITE_INITIALE
    or_public: int = OR_PUBLIC_INITIAL
    active_role_ids: list[str] = field(default_factory=list)
    players: dict[str, PlayerState] = field(default_factory=dict)
    deck: list[str] = field(default_factory=list)
    deck_position: int = 0
    current_card_id: str | None = None
    current_card_revealed: dict | None = None
    decisions: dict[str, str] = field(default_factory=dict)
    interieur_falsifie: bool = False
    log: list[str] = field(default_factory=list)
    winner_uid: str | None = None
    final_scores: dict | None = None
    host_uid: str | None = None

    # ---------- Setup ----------

    def add_player(self, player_uid: str, sid: str, pseudo: str) -> PlayerState:
        if player_uid in self.players:
            p = self.players[player_uid]
            p.sid = sid
            p.connected = True
            return p
        is_first = len(self.players) == 0
        p = PlayerState(sid=sid, player_uid=player_uid, pseudo=pseudo, is_king=False)
        self.players[player_uid] = p
        if is_first:
            self.host_uid = player_uid
        return p

    def assign_roles(self, king_uid: str) -> None:
        total = len(self.players)
        n_ministres = nb_ministres_for(total)
        chosen_roles = random.sample(ROLE_ORDER, n_ministres)
        self.active_role_ids = chosen_roles

        non_king_uids = [uid for uid in self.players if uid != king_uid]
        random.shuffle(non_king_uids)

        for uid in self.players:
            self.players[uid].is_king = (uid == king_uid)
            self.players[uid].role_ids = []

        for uid, role_id in zip(non_king_uids, chosen_roles):
            self.players[uid].role_ids = [role_id]

        self.deck = build_shuffled_deck()
        self.deck_position = 0
        self.phase = Phase.DISCUSSION
        self.cycle = 1
        self.log.append(
            f"Rôles distribués : {', '.join(ROLES[r].name for r in chosen_roles)}. "
            f"{self.players[king_uid].pseudo} est désigné Roi."
        )

    def ministre_uid_for_role(self, role_id: str) -> str | None:
        for uid, p in self.players.items():
            if role_id in p.role_ids:
                return uid
        return None

    def uids_libres(self) -> list[str]:
        return [uid for uid, p in self.players.items() if not p.role_ids and not p.is_king]

    def uids_ministres_actifs(self) -> list[str]:
        return [uid for uid, p in self.players.items() if p.role_ids and not p.is_king]

    def king_uid(self) -> str | None:
        for uid, p in self.players.items():
            if p.is_king:
                return uid
        return None

    # ---------- Cycle flow ----------

    def start_decision_phase(self) -> None:
        self.decisions = {}
        self.interieur_falsifie = False
        self.current_card_id = None
        self.current_card_revealed = None
        self.phase = Phase.DECISION
        self.log.append(f"--- Cycle {self.cycle}/{NB_CYCLES} : les ministres délibèrent ---")

    def submit_decision(self, role_id: str, choice: str) -> None:
        assert choice in ("ouvrir", "fermer")
        self.decisions[role_id] = choice

    def all_decisions_in(self) -> bool:
        return all(r in self.decisions for r in self.active_role_ids)

    def draw_crisis_card(self) -> dict | None:
        if self.deck_position >= len(self.deck):
            self.deck = build_shuffled_deck()
            self.deck_position = 0
        card_id = self.deck[self.deck_position]
        self.deck_position += 1
        self.current_card_id = card_id
        card = CARD_BY_ID[card_id]
        self.current_card_revealed = {
            "titre": card.titre,
            "texte": card.texte,
            "stabilite_delta": card.stabilite_delta,
            "or_public_delta": card.or_public_delta,
        }
        self.phase = Phase.RAPPORT
        return self.current_card_revealed

    def falsify_report(self, stabilite_annoncee: int, or_annonce: int) -> None:
        if self.current_card_revealed is None:
            return
        self.interieur_falsifie = True
        self.current_card_revealed["stabilite_delta"] = stabilite_annoncee
        self.current_card_revealed["or_public_delta"] = or_annonce

    # ---------- Résolution ----------

    def resolve_cycle(self) -> dict:
        summary = {"cycle": self.cycle, "effets": [], "carte": None, "ruine": False}

        real_card = CARD_BY_ID.get(self.current_card_id) if self.current_card_id else None
        if real_card:
            self.stabilite += real_card.stabilite_delta
            self.or_public += real_card.or_public_delta
            summary["carte"] = {
                "titre": real_card.titre,
                "annonce": self.current_card_revealed,
                "reel": {
                    "stabilite_delta": real_card.stabilite_delta,
                    "or_public_delta": real_card.or_public_delta,
                },
                "falsifie": self.interieur_falsifie,
            }

        connetable_ouvert = self.decisions.get("connetable") == "ouvrir"

        for role_id in self.active_role_ids:
            choice = self.decisions.get(role_id, "ouvrir")
            uid = self.ministre_uid_for_role(role_id)
            if uid is None:
                continue
            p = self.players[uid]
            effet = self._apply_robinet_effect(role_id, choice, p, connetable_ouvert)
            summary["effets"].append({
                "role_id": role_id,
                "role_name": ROLES[role_id].name,
                "pseudo": p.pseudo,
                "choice": choice,
                **effet,
            })

        self.stabilite = max(0, min(STABILITE_MAX, self.stabilite))

        if self.stabilite <= 0 or self.or_public < 0:
            self.phase = Phase.TERMINEE_RUINE
            summary["ruine"] = True
            self.log.append("💀 LE ROYAUME S'EFFONDRE — tous les joueurs perdent la partie.")
            return summary

        self.log.append(
            f"Cycle {self.cycle} résolu — Stabilité: {self.stabilite}, Caisses: {self.or_public}"
        )

        if self.cycle >= NB_CYCLES:
            self._compute_final_scores()
            self.phase = Phase.TERMINEE_VICTOIRE
        else:
            self.cycle += 1
            self.phase = Phase.RESOLUTION

        return summary

    def _apply_robinet_effect(self, role_id: str, choice: str, p: PlayerState,
                                connetable_ouvert: bool) -> dict:
        rng = random.Random()
        d = {"stabilite_delta": 0, "or_public_delta": 0, "or_personnel_delta": 0, "influence_delta": 0}

        if role_id == "interieur":
            if choice == "ouvrir":
                d["influence_delta"] = 3
                d["stabilite_delta"] = 1
            else:
                d["or_personnel_delta"] = 9
                if rng.random() < 0.55:
                    d["stabilite_delta"] = -9

        elif role_id == "finances":
            if choice == "ouvrir":
                d["or_public_delta"] = 4
                if rng.random() < 0.3:
                    d["or_personnel_delta"] = 4
            else:
                d["or_personnel_delta"] = 13
                d["or_public_delta"] = -15 if not connetable_ouvert else -10

        elif role_id == "aumonier":
            if choice == "ouvrir":
                d["stabilite_delta"] = 3
            else:
                d["or_personnel_delta"] = 11
                d["stabilite_delta"] = -11

        elif role_id == "subsistances":
            if choice == "ouvrir":
                d["or_public_delta"] = 2
                d["stabilite_delta"] = 1
            else:
                d["or_personnel_delta"] = 10
                d["stabilite_delta"] = -6
                d["or_public_delta"] = -5

        elif role_id == "connetable":
            if choice == "ouvrir":
                d["influence_delta"] = 2
            else:
                d["or_personnel_delta"] = 8

        if role_id != "connetable" and not connetable_ouvert and rng.random() < 0.25:
            steal = min(p.or_personnel, 5)
            if steal > 0:
                d["or_personnel_delta"] -= steal
                d["vol_subi"] = steal

        p.or_personnel = max(0, p.or_personnel + d["or_personnel_delta"])
        p.influence = max(0, p.influence + d["influence_delta"])
        self.stabilite += d["stabilite_delta"]
        self.or_public += d["or_public_delta"]

        return d

    def _compute_final_scores(self) -> None:
        scores = {}
        for uid, p in self.players.items():
            if not p.role_ids:
                continue
            statut = sum(ROLES[r].statut_ministere for r in p.role_ids)
            roles_noms = " + ".join(ROLES[r].name for r in p.role_ids)
            puissance = p.or_personnel + p.influence + statut * self.stabilite
            scores[uid] = {
                "pseudo": p.pseudo,
                "role": roles_noms,
                "or_personnel": p.or_personnel,
                "influence": p.influence,
                "statut_ministere": statut,
                "stabilite_finale": self.stabilite,
                "puissance": puissance,
            }
        self.final_scores = scores
        if scores:
            self.winner_uid = max(scores, key=lambda u: scores[u]["puissance"])

    # ---------- Démission / Révocation ----------

    def resign(self, role_id: str, successor_uid: str) -> bool:
        """
        Démission : le portefeuille est transféré à UNE seule personne
        désignée, jamais le Roi. Le successeur peut être libre (il devient
        ministre) ou déjà ministre (il cumule alors les deux postes).
        """
        if successor_uid == self.king_uid():
            return False
        successor = self.players.get(successor_uid)
        if successor is None:
            return False

        old_uid = self.ministre_uid_for_role(role_id)
        if old_uid is None or old_uid == successor_uid:
            return False

        self.players[old_uid].role_ids.remove(role_id)
        self.players[old_uid].resigned_roles.append(role_id)
        if role_id not in successor.role_ids:
            successor.role_ids.append(role_id)

        cumul = " (cumul de portefeuilles)" if len(successor.role_ids) > 1 else ""
        self.log.append(
            f"💥 {self.players[old_uid].pseudo} démissionne du poste de "
            f"{ROLES[role_id].name} ! Transféré à {successor.pseudo}{cumul}."
        )
        return True

    def revoke(self, role_id: str, successor_uid: str, requester_uid: str) -> bool:
        """Révocation royale : seul le Roi peut destituer et nommer un successeur."""
        if requester_uid != self.king_uid():
            return False
        if successor_uid == self.king_uid():
            return False
        successor = self.players.get(successor_uid)
        if successor is None:
            return False

        old_uid = self.ministre_uid_for_role(role_id)
        if old_uid is None or old_uid == successor_uid:
            return False

        self.players[old_uid].role_ids.remove(role_id)
        self.players[old_uid].resigned_roles.append(role_id)
        if role_id not in successor.role_ids:
            successor.role_ids.append(role_id)

        cumul = " (cumul de portefeuilles)" if len(successor.role_ids) > 1 else ""
        self.log.append(
            f"👑 Le Roi destitue {self.players[old_uid].pseudo} du poste de "
            f"{ROLES[role_id].name} ! Nommé successeur : {successor.pseudo}{cumul}."
        )
        return True

    # ---------- Sérialisation ----------

    def to_dict(self) -> dict:
        return {
            "room_code": self.room_code,
            "phase": self.phase.value,
            "cycle": self.cycle,
            "stabilite": self.stabilite,
            "or_public": self.or_public,
            "active_role_ids": self.active_role_ids,
            "players": {uid: p.to_public_dict() for uid, p in self.players.items()},
            "deck": self.deck,
            "deck_position": self.deck_position,
            "current_card_id": self.current_card_id,
            "current_card_revealed": self.current_card_revealed,
            "decisions": self.decisions,
            "interieur_falsifie": self.interieur_falsifie,
            "log": self.log[-50:],
            "winner_uid": self.winner_uid,
            "final_scores": self.final_scores,
            "host_uid": self.host_uid,
        }

    @staticmethod
    def from_dict(data: dict) -> "GameState":
        gs = GameState(room_code=data["room_code"])
        gs.phase = Phase(data["phase"])
        gs.cycle = data["cycle"]
        gs.stabilite = data["stabilite"]
        gs.or_public = data["or_public"]
        gs.active_role_ids = data["active_role_ids"]
        gs.players = {uid: PlayerState(**pdata) for uid, pdata in data["players"].items()}
        gs.deck = data["deck"]
        gs.deck_position = data["deck_position"]
        gs.current_card_id = data["current_card_id"]
        gs.current_card_revealed = data["current_card_revealed"]
        gs.decisions = data["decisions"]
        gs.interieur_falsifie = data["interieur_falsifie"]
        gs.log = data["log"]
        gs.winner_uid = data["winner_uid"]
        gs.final_scores = data["final_scores"]
        gs.host_uid = data["host_uid"]
        return gs


# ============================================================================
# 4. PERSISTANCE (SQLite local / Postgres si DATABASE_URL est définie)
# ============================================================================

_USE_POSTGRES = bool(os.environ.get("DATABASE_URL"))
_db_lock = threading.Lock()

if _USE_POSTGRES:
    import psycopg2

SQLITE_PATH = os.environ.get("SQLITE_PATH", os.path.join(os.path.dirname(__file__), "game.db"))


def get_connection():
    if _USE_POSTGRES:
        return psycopg2.connect(os.environ["DATABASE_URL"], sslmode=os.environ.get("PGSSLMODE", "require"))
    conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _db_lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            ts_type = "TIMESTAMP" if _USE_POSTGRES else "TEXT"
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS games (
                    room_code TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at {ts_type} NOT NULL
                )
            """)
            conn.commit()
        finally:
            conn.close()


def save_game(room_code: str, state_dict: dict) -> None:
    payload = json.dumps(state_dict, ensure_ascii=False)
    now = datetime.now(timezone.utc)
    with _db_lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            if _USE_POSTGRES:
                cur.execute("""
                    INSERT INTO games (room_code, state_json, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (room_code) DO UPDATE
                    SET state_json = EXCLUDED.state_json, updated_at = EXCLUDED.updated_at
                """, (room_code, payload, now))
            else:
                cur.execute("""
                    INSERT INTO games (room_code, state_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(room_code) DO UPDATE
                    SET state_json = excluded.state_json, updated_at = excluded.updated_at
                """, (room_code, payload, now.isoformat()))
            conn.commit()
        finally:
            conn.close()


def load_game(room_code: str) -> dict | None:
    with _db_lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            placeholder = "%s" if _USE_POSTGRES else "?"
            cur.execute(f"SELECT state_json FROM games WHERE room_code = {placeholder}", (room_code,))
            row = cur.fetchone()
            return json.loads(row[0]) if row else None
        finally:
            conn.close()


def room_exists_in_db(room_code: str) -> bool:
    with _db_lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            placeholder = "%s" if _USE_POSTGRES else "?"
            cur.execute(f"SELECT 1 FROM games WHERE room_code = {placeholder}", (room_code,))
            return cur.fetchone() is not None
        finally:
            conn.close()


# ---------- Salons en mémoire (cache rapide, synchronisé avec la DB) ----------

_rooms: dict[str, GameState] = {}
_rooms_lock = threading.Lock()
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # sans I/O/0/1


def generate_room_code(length: int = 5) -> str:
    while True:
        code = "".join(random.choice(CODE_ALPHABET) for _ in range(length))
        with _rooms_lock:
            in_memory = code in _rooms
        if not in_memory and not room_exists_in_db(code):
            return code


def create_room() -> GameState:
    code = generate_room_code()
    gs = GameState(room_code=code)
    with _rooms_lock:
        _rooms[code] = gs
    persist(gs)
    return gs


def get_room(room_code: str) -> GameState | None:
    room_code = room_code.upper()
    with _rooms_lock:
        gs = _rooms.get(room_code)
    if gs is not None:
        return gs
    data = load_game(room_code)
    if data is None:
        return None
    gs = GameState.from_dict(data)
    with _rooms_lock:
        _rooms[room_code] = gs
    return gs


def persist(gs: GameState) -> None:
    save_game(gs.room_code, gs.to_dict())


# ============================================================================
# 5. APPLICATION FLASK + SOCKETIO
# ============================================================================

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent")
init_db()

_sid_index: dict[str, tuple[str, str]] = {}  # sid -> (room_code, player_uid)


def public_state(gs: GameState) -> dict:
    """État envoyé à tous : les décisions en cours restent masquées (vote secret)."""
    d = gs.to_dict()
    if gs.phase == Phase.DECISION:
        d["decisions"] = {role: True for role in gs.decisions}
    return d


def roles_catalog() -> dict:
    return {
        "king": KING_ROLE,
        "roles": {rid: {
            "id": r.id, "name": r.name, "icon": r.icon, "color": r.color,
            "flux": r.flux, "voie_serviteur": r.voie_serviteur, "voie_fourbe": r.voie_fourbe,
            "ouvrir_desc": r.ouvrir_desc, "fermer_desc": r.fermer_desc,
            "statut_ministere": r.statut_ministere,
        } for rid, r in ROLES.items()},
        "role_order": ROLE_ORDER,
        "nb_cycles": NB_CYCLES,
    }


def broadcast_state(gs: GameState) -> None:
    persist(gs)
    socketio.emit("state_update", public_state(gs), room=gs.room_code)


# ---------------------------------------------------------------------------
# Routes HTTP
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/room/<room_code>")
def room_page(room_code):
    return render_template("room.html", room_code=room_code.upper())


@app.route("/regles")
def regles_page():
    return render_template("regles.html")


@app.route("/api/roles")
def api_roles():
    return jsonify(roles_catalog())


@app.route("/healthz")
def healthz():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Événements SocketIO
# ---------------------------------------------------------------------------

@socketio.on("create_room")
def on_create_room(data):
    pseudo = (data or {}).get("pseudo", "").strip()[:24] or "Joueur"
    gs = create_room()
    player_uid = str(uuid.uuid4())
    gs.add_player(player_uid, request.sid, pseudo)
    join_room(gs.room_code)
    _sid_index[request.sid] = (gs.room_code, player_uid)
    emit("room_joined", {"room_code": gs.room_code, "player_uid": player_uid, "is_host": True})
    broadcast_state(gs)


@socketio.on("join_room_event")
def on_join_room(data):
    data = data or {}
    pseudo = (data.get("pseudo") or "").strip()[:24] or "Joueur"
    room_code = (data.get("room_code") or "").strip().upper()
    existing_uid = data.get("player_uid")

    gs = get_room(room_code)
    if gs is None:
        emit("join_error", {"message": "Ce salon n'existe pas. Vérifiez le code."})
        return

    if existing_uid and existing_uid in gs.players:
        player_uid = existing_uid
        gs.players[player_uid].sid = request.sid
        gs.players[player_uid].connected = True
    else:
        if gs.phase != Phase.LOBBY:
            emit("join_error", {"message": "La partie a déjà commencé, impossible de rejoindre."})
            return
        if len(gs.players) >= MAX_TOTAL_PLAYERS:
            emit("join_error", {"message": f"Le salon est complet ({MAX_TOTAL_PLAYERS} joueurs max)."})
            return
        player_uid = str(uuid.uuid4())
        gs.add_player(player_uid, request.sid, pseudo)

    join_room(room_code)
    _sid_index[request.sid] = (room_code, player_uid)
    emit("room_joined", {"room_code": gs.room_code, "player_uid": player_uid, "is_host": gs.host_uid == player_uid})
    broadcast_state(gs)


@socketio.on("start_game")
def on_start_game(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None or gs.host_uid != player_uid:
        emit("action_error", {"message": "Seul l'hôte peut démarrer la partie."})
        return
    if len(gs.players) < MIN_TOTAL_PLAYERS:
        emit("action_error", {"message": f"Il faut au moins {MIN_TOTAL_PLAYERS} joueurs (1 Roi + 2 ministres)."})
        return
    if gs.phase != Phase.LOBBY:
        return

    king_uid = (data or {}).get("king_uid") or player_uid
    if king_uid not in gs.players:
        king_uid = player_uid

    gs.assign_roles(king_uid)
    gs.start_decision_phase()
    broadcast_state(gs)


@socketio.on("submit_decision")
def on_submit_decision(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None or gs.phase != Phase.DECISION:
        return

    choice = (data or {}).get("choice")
    role_id = (data or {}).get("role_id")
    if choice not in ("ouvrir", "fermer"):
        return

    player = gs.players.get(player_uid)
    if player is None or role_id not in player.role_ids:
        emit("action_error", {"message": "Vous ne contrôlez pas ce portefeuille."})
        return

    gs.submit_decision(role_id, choice)

    if gs.all_decisions_in():
        if "interieur" in gs.active_role_ids:
            gs.draw_crisis_card()
            broadcast_state(gs)
            return
        else:
            summary = gs.resolve_cycle()
            broadcast_state(gs)
            socketio.emit("cycle_resolved", summary, room=gs.room_code)
            return

    broadcast_state(gs)

@socketio.on("falsify_report")
def on_falsify_report(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None or gs.phase != Phase.RAPPORT:
        return

    player = gs.players.get(player_uid)
    if player is None or "interieur" not in player.role_ids:
        return

    if gs.decisions.get("interieur") == "fermer":
        stab = int((data or {}).get("stabilite_annoncee", 0))
        org = int((data or {}).get("or_annonce", 0))
        gs.falsify_report(stab, org)

    summary = gs.resolve_cycle()
    broadcast_state(gs)
    socketio.emit("cycle_resolved", summary, room=gs.room_code)


@socketio.on("confirm_report")
def on_confirm_report(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None or gs.phase != Phase.RAPPORT:
        return
    player = gs.players.get(player_uid)
    if player is None or "interieur" not in player.role_ids:
        return

    summary = gs.resolve_cycle()
    broadcast_state(gs)
    socketio.emit("cycle_resolved", summary, room=gs.room_code)


@socketio.on("next_cycle")
def on_next_cycle(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None or gs.phase != Phase.RESOLUTION:
        return
    gs.start_decision_phase()
    broadcast_state(gs)


@socketio.on("resign")
def on_resign(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None:
        return

    role_id = (data or {}).get("role_id")
    successor_uid = (data or {}).get("successor_uid")
    player = gs.players.get(player_uid)
    if player is None or role_id not in player.role_ids:
        emit("action_error", {"message": "Vous ne pouvez démissionner que de votre propre poste."})
        return

    ok = gs.resign(role_id, successor_uid)
    if not ok:
        emit("action_error", {"message": "Démission impossible : successeur invalide (ne peut pas être le Roi)."})
        return
    broadcast_state(gs)


@socketio.on("revoke")
def on_revoke(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None:
        return

    role_id = (data or {}).get("role_id")
    successor_uid = (data or {}).get("successor_uid")

    ok = gs.revoke(role_id, successor_uid, requester_uid=player_uid)
    if not ok:
        emit("action_error", {"message": "Révocation impossible (seul le Roi peut révoquer)."})
        return
    broadcast_state(gs)


@socketio.on("send_chat")
def on_send_chat(data):
    info = _sid_index.get(request.sid)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None:
        return
    player = gs.players.get(player_uid)
    if player is None:
        return
    message = (data or {}).get("message", "").strip()[:500]
    if not message:
        return
    socketio.emit("chat_message", {
        "pseudo": player.pseudo,
        "message": message,
        "is_king": player.is_king,
    }, room=room_code)


@socketio.on("disconnect")
def on_disconnect():
    info = _sid_index.pop(request.sid, None)
    if not info:
        return
    room_code, player_uid = info
    gs = get_room(room_code)
    if gs is None:
        return
    player = gs.players.get(player_uid)
    if player:
        player.connected = False
        broadcast_state(gs)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)

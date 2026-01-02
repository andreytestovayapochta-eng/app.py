import os
import logging
import asyncio
from dotenv import load_dotenv
import random
import datetime
import json
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode, ChatType
from aiogram.client.bot import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiohttp
from aiohttp import ClientSession
from aiohttp_socks import ProxyConnector
from aiogram.methods import TelegramMethod # <--- ЭТА СТРОКА ДОЛЖНА БЫТЬ ДОБАВЛЕНА
from aiogram.types import User 

from database import init_db, Session, Game, Player, Group
# ИМПОРТ НОВЫХ КОНСТАНТ
from utils.constants import (
    PHASE_DURATIONS, MIN_PLAYERS_TO_START, ROLES_CONFIG, ROLE_NAMES_RU,
    ROLE_EMOJIS, GENDER_EMOJIS, FACTION_EMOJIS, PHASE_EMOJIS, RESULT_EMOJIS,
    NIGHT_ANIMATION_FILE_ID, DAY_ANIMATION_FILE_ID,
    BASE_EXP_FOR_LEVEL_UP, LEVEL_UP_EXP_INCREMENT, EXP_FOR_WIN, EXP_FOR_PARTICIPATION,
    DOLLARS_FOR_WIN, DOLLARS_FOR_PARTICIPATION, DOLLARS_FOR_LOSS,
    BASE_GROUP_EXP_FOR_LEVEL_UP, GROUP_LEVEL_UP_EXP_INCREMENT,
    GROUP_EXP_FOR_GAME_END, GROUP_EXP_PER_PLAYER_BONUS,
    DOLLAR_TO_GROUP_EXP_RATIO, DIAMOND_TO_GROUP_EXP_RATIO,
    GROUP_LEVEL_BONUSES,
    CUSTOM_FRAMES, UNLOCKED_FRAMES_DEFAULT, FRAME_PRICES,
    CUSTOM_TITLES, UNLOCKED_TITLES_DEFAULT, TITLE_PRICES
)

# Загружаем переменные окружения
# Загружаем переменные окружения
load_dotenv()

# Получаем токен бота из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable not set.")

BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))

PROXY_URL = os.getenv("TELEGRAM_PROXY_URL")

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Инициализация планировщика задач
scheduler = AsyncIOScheduler()

# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ БОТА И ДИСПЕТЧЕРА, которые будут ИНИЦИАЛИЗИРОВАНЫ В main/start_application
bot: Bot = None
dp: Dispatcher = None

# Переменная для хранения информации о боте (для использования bot.me.username)
bot_self_info = None

# НОВАЯ ВАЖНАЯ КОНСТАНТА - ID БОТА, чтобы не путать его с игроками
BOT_ID = None # Будет установлено при запуске бота


# --- FSM для прощальных сообщений и ночных действий и пожертвований ---
class GameState(StatesGroup):
    waiting_for_farewell_message = State()
    waiting_for_faction_message = State()
    waiting_for_donate_group_selection = State()
    waiting_for_donate_currency_selection = State()
    waiting_for_donate_dollars_amount = State()
    waiting_for_donate_diamonds_amount = State()
    
    # НОВЫЕ СОСТОЯНИЯ ДЛЯ КАСТОМИЗАЦИИ
    waiting_for_frame_selection = State() # Ожидание выбора рамки из списка
    waiting_for_title_selection = State() # Ожидание выбора титула из списка
    waiting_for_frame_preview_action = State() # НОВОЕ СОСТОЯНИЕ ДЛЯ ПРЕДПРОСМОТРА РАМКИ
    waiting_for_title_preview_action = State() # НОВОЕ СОСТОЯНИЕ ДЛЯ ПРЕДПРОСМОТРА ТИТУЛА
def get_exp_for_next_level(current_level: int) -> int:
    """Рассчитывает количество опыта, необходимое для перехода на следующий уровень."""
    return BASE_EXP_FOR_LEVEL_UP + (current_level * LEVEL_UP_EXP_INCREMENT)

# --- Вспомогательные функции для игры ---
def get_roles_distribution(num_players: int) -> dict[str, int]:
    """
    Определяет распределение ролей в зависимости от количества игроков.
    Возвращает словарь {role_name: count}.
    Использует ROLES_CONFIG из constants.py
    """
    if num_players < MIN_PLAYERS_TO_START:
        return {}

    # Получаем базовую конфигурацию для данного количества игроков
    roles_config_current = ROLES_CONFIG.get(num_players)

    if roles_config_current is None:
        # Если нет точной конфигурации, используем логику для 9+ игроков (если ROLES_CONFIG не содержит)
        # Это старая логика, которую можно улучшить, если ROLES_CONFIG будет покрывать все случаи
        roles_config_current = {'don': 1, 'commissioner': 1, 'doctor': 1, 'maniac': 1}
        
        # Добавляем мафию, если игроков больше 4 (4 - это уже дон, ком, док, маньяк)
        num_mafia_additional = (num_players - 4) // 3
        roles_config_current['mafia'] = num_mafia_additional if num_mafia_additional > 0 else 0
        
        current_roles_count = sum(roles_config_current.values())
        if current_roles_count < num_players:
            roles_config_current['civilian'] = num_players - current_roles_count
        else:
            roles_config_current['civilian'] = 0 # если вдруг роли заполнились, а мирных нет

    return {role: count for role, count in roles_config_current.items() if count > 0}

# --- НОВАЯ ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ---
def format_group_info(group_obj: Group) -> str:
    """Форматирует информацию о группе для вывода."""
    if not group_obj:
        return "Информация о группе недоступна."

        # Убедитесь, что здесь используются именно group_obj.level, group_obj.experience и т.д.
        # И что они не округляются или не преобразуются неверно.
    group_info_text = (
        f"{FACTION_EMOJIS['town']} Город: <b>{group_obj.name}</b> (Уровень {group_obj.level})\n"
        f"{FACTION_EMOJIS['experience']} Опыт: {group_obj.experience:.0f} (до след. ур: {max(0, get_exp_for_next_level(group_obj.level) - group_obj.experience):.0f})\n" # Исправлено
        f"{FACTION_EMOJIS['bonus']} Бонусы: +{group_obj.bonus_exp_percent*100:.0f}% EXP, +{group_obj.bonus_dollars_percent*100:.0f}% $\n"
        f"{FACTION_EMOJIS['donate']} Пожертвовано: {group_obj.dollars_donated} Долларов, {group_obj.diamonds_donated:.2f} Бриллиантов\n"
    )
    return group_info_text
# --- КОНЕЦ НОВОЙ ВСПОМОГАТЕЛЬНОЙ ФУНКЦИИ ---

# --- Вспомогательные функции для смены фаз ---
async def end_day_phase(game_id: int):
    """Завершает фазу дня и начинает фазу голосования."""
    with Session() as session:
        try:
            game = session.get(Game, game_id)
            if not game or game.status != 'playing' or game.phase != 'day':
                logging.warning(f"end_day_phase: Game {game_id} not in day phase or not playing (status={game.status}, phase={game.phase}).")
                return

            game.phase = 'voting'
            game.phase_end_time = datetime.datetime.now() + datetime.timedelta(seconds=PHASE_DURATIONS['voting'])
            session.add(game)
            session.commit()
            logging.info(f"Game {game_id} transitioned to 'voting' phase.")

            players_alive_in_game = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()

            for player_in_game in players_alive_in_game:
                keyboard_buttons = []
                possible_targets = [p for p in players_alive_in_game if p.id != player_in_game.id]

                for target_player in possible_targets:
                    keyboard_buttons.append(InlineKeyboardButton(text=target_player.full_name, callback_data=f"vote_{game.id}_{target_player.id}"))

                if keyboard_buttons:
                    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        keyboard_buttons[i:i + 2] for i in range(0, len(keyboard_buttons), 2)
                    ])
                    try:
                        await bot.send_message(
                            chat_id=player_in_game.user_id,
                            text=f"Наступила <b>Фаза голосования!</b> {PHASE_EMOJIS['voting']}\n"
                                 f"У вас есть {PHASE_DURATIONS['voting']} секунд, чтобы проголосовать. Выберите игрока, которого хотите казнить.",
                            reply_markup=inline_keyboard,
                            parse_mode=ParseMode.HTML
                        )
                    except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован игроком {player_in_game.full_name} ({player_in_game.user_id}) в игре {game.id}. Не удалось отправить кнопки голосования.")
                        await bot.send_message(game.chat_id,
                                               f"ВНИМАНИЕ: Не удалось отправить кнопки голосования игроку <a href='tg://user?id={player_in_game.user_id}'>{player_in_game.full_name}</a>. Пожалуйста, попросите его написать боту /start в ЛС, чтобы он мог принимать сообщения.",
                                               parse_mode=ParseMode.HTML)
                    except Exception as e:
                        logging.error(f"Не удалось отправить кнопки голосования игроку {player_in_game.full_name} ({player_in_game.user_id}) для game {game.id}: {e}", exc_info=True)
                        await bot.send_message(game.chat_id,
                                               f"ВНИМАНИЕ: Не удалось отправить кнопки голосования игроку <a href='tg://user?id={player_in_game.user_id}'>{player_in_game.full_name}</a>. Пожалуйста, попросите его написать боту /start в ЛС, чтобы он мог принимать сообщения.",
                                               parse_mode=ParseMode.HTML)
                else:
                    try:
                        await bot.send_message(
                            chat_id=player_in_game.user_id,
                            text=f"Наступила <b>Фаза голосования!</b> {PHASE_EMOJIS['voting']}\n"
                                 f"К сожалению, сейчас нет доступных целей для голосования (возможно, вы единственный оставшийся игрок).",
                            parse_mode=ParseMode.HTML
                        )
                    except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован игроком {player_in_game.full_name}. Не удалось отправить сообщение о недоступности голосования.")
                    except Exception as e:
                        logging.warning(f"Не удалось отправить сообщение о недоступности голосования игроку {player_in_game.full_name} ({player_in_game.user_id}) для game {game.id}: {e}")

            keyboard_chat_link = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Перейти в ЛС бота",
                                      url=f"https://t.me/{bot_self_info.username if bot_self_info else 'your_bot_username'}?start=game_{game.id}")]
            ])
            await bot.send_message(
                chat_id=game.chat_id,
                text=f"Наступила <b>Фаза голосования!</b> {PHASE_EMOJIS['voting']}\n"
                     f"Пришло время найти и наказать виновных....\n"
                     f"Голосование продлится {PHASE_DURATIONS['voting']} секунд. Проверьте свои личные сообщения от бота и проголосуйте там. "
                     f"Используйте кнопку ниже, чтобы перейти в ЛС бота, если нужно.",
                reply_markup=keyboard_chat_link,
                parse_mode=ParseMode.HTML
            )

            scheduler.add_job(end_voting_phase, 'date', run_date=game.phase_end_time, args=[game_id],
                              id=f"end_voting_game_{game_id}")
            logging.info(f"Scheduled end_voting_phase for game {game_id} at {game.phase_end_time}")

        except Exception as e:
            logging.error(f"Ошибка в end_day_phase для game_id {game_id}: {e}", exc_info=True)
            session.rollback()


async def end_voting_phase(game_id: int):
    """Завершает фазу голосования, обрабатывает голоса и начинает фазу казни."""
    with Session() as session:
        try:
            game = session.get(Game, game_id)
            if not game or game.status != 'playing' or game.phase != 'voting':
                logging.warning(f"end_voting_phase: Game {game_id} not in voting phase or not playing.")
                return

            players = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()

            vote_counts = {}
            for voter in players:
                if voter.voted_for_player_id:
                    target_player = session.get(Player, voter.voted_for_player_id)
                    if target_player and target_player.is_alive:
                        # Учитываем вес голоса Дона, если он есть
                        if voter.role == 'don':
                            vote_counts[target_player.id] = vote_counts.get(target_player.id, 0) + 3 # Голос Дона весит 3
                        else:
                            vote_counts[target_player.id] = vote_counts.get(target_player.id, 0) + 1

            max_votes = 0
            players_with_max_votes = []

            for target_id, count in vote_counts.items():
                if count > max_votes:
                    max_votes = count
                    players_with_max_votes = [target_id]
                elif count == max_votes and max_votes > 0:
                    players_with_max_votes.append(target_id)
            
            player_to_lynch = None

            if len(players_with_max_votes) == 1:
                executed_player_for_lynch_id = players_with_max_votes[0]
                player_to_lynch = session.get(Player, executed_player_for_lynch_id)

                if player_to_lynch:
                    game.phase = 'lynch_vote'
                    game.voted_for_player_id = player_to_lynch.id
                    game.lynch_vote_likes = 0
                    game.lynch_vote_dislikes = 0
                    game.phase_end_time = datetime.datetime.now() + datetime.timedelta(seconds=PHASE_DURATIONS['lynch_vote'])
                    session.add(game)
                    session.commit()
                    logging.info(f"Game {game_id} transitioned to 'lynch_vote' phase for player {player_to_lynch.full_name}.")
                    
                    keyboard_lynch = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=f"КАЗНИТЬ {FACTION_EMOJIS['mafia']}", callback_data=f"lynch_like_{game.id}_{player_to_lynch.id}")],
                        [InlineKeyboardButton(text=f"ПОМИЛОВАТЬ {FACTION_EMOJIS['town']}", callback_data=f"lynch_dislike_{game.id}_{player_to_lynch.id}")]
                    ])

                    initial_lynch_text = (
                        f"{PHASE_EMOJIS['lynch_vote']} <b>Фаза суда!</b> Жители, вы проголосовали за <a href='tg://user?id={player_to_lynch.user_id}'><b>{player_to_lynch.full_name}</b></a>.\n"
                        f"У вас есть {PHASE_DURATIONS['lynch_vote']} секунд, чтобы решить: <b>казнить его или помиловать?</b>\n"
                        f"<i>(Если никто не проголосует, его казнят. Если будет ничья — помилуют)</i>\n\n"
                        f"{FACTION_EMOJIS['mafia']} За казнь: <b>0</b> | {FACTION_EMOJIS['town']} Против казни: <b>0</b>"
                    )

                    lynch_msg = await bot.send_message(chat_id=game.chat_id,
                        text=initial_lynch_text,
                        reply_markup=keyboard_lynch,
                        parse_mode=ParseMode.HTML
                    )
                    game.lynch_message_id = lynch_msg.message_id
                    game.lynch_voters = ""
                    session.add(game)
                    session.commit()
                    logging.info(f"Game {game_id} transitioned to 'lynch_vote' phase for player {player_to_lynch.full_name}.")
                    scheduler.add_job(end_lynch_voting_phase, 'date', run_date=game.phase_end_time, args=[game_id],
                                      id=f"end_lynch_vote_game_{game_id}")
                    logging.info(f"Scheduled end_lynch_voting_phase for game {game_id} at {game.phase_end_time}")
            else: # Ничья или нет голосов -> Никого не казнят, сразу переход к ночи
                outcome_text = (
                    f"была ничья, поэтому никто не казнен. {RESULT_EMOJIS['missed']}" 
                    if max_votes > 0 
                    else f"никто не был казнен. {RESULT_EMOJIS['missed']}"
                )
                await bot.send_message(
                    chat_id=game.chat_id,
                    text=f"{PHASE_EMOJIS['voting']} Голосование завершено. По итогам голосования, {outcome_text}"
                )
                await prepare_for_night_phase(game_id, session)

        except Exception as e:
            logging.error(f"Ошибка в end_voting_phase для game_id {game_id}: {e}", exc_info=True)
            session.rollback()

async def end_lynch_voting_phase(game_id: int):
    """Завершает фазу голосования за казнь, обрабатывает голоса и начинает фазу ночи."""
    with Session() as session:
        try:
            game = session.get(Game, game_id)
            if not game or game.status != 'playing' or game.phase != 'lynch_vote':
                logging.warning(f"end_lynch_voting_phase: Game {game_id} not in lynch_vote phase or not playing.")
                return

            executed_player = session.get(Player, game.voted_for_player_id)
            if not executed_player:
                logging.error(f"end_lynch_voting_phase: No player found for lynch_vote_id {game.voted_for_player_id} in game {game.id}.")
                await bot.send_message(game.chat_id, "Ошибка: Не удалось найти игрока для казни.")
                await prepare_for_night_phase(game.id, session)
                return

            lynch_likes = game.lynch_vote_likes
            lynch_dislikes = game.lynch_vote_dislikes

            perform_execution = False
            if lynch_likes > lynch_dislikes:
                perform_execution = True
            elif lynch_likes == 0 and lynch_dislikes == 0:  # Никто не проголосовал, по умолчанию казнить
                perform_execution = True
            # Если lynch_likes <= lynch_dislikes (и не оба 0), то помиловать

            execution_message = ""
            game_ended_after_lynch = False

            if perform_execution:
                executed_player.is_alive = False
                executed_player.total_deaths = (executed_player.total_deaths or 0) + 1 # Увеличиваем счетчик смертей
                session.add(executed_player)
                # Нет необходимости коммитить здесь, будет в конце функции

                killed_player_role_ru = ROLE_NAMES_RU.get(executed_player.role, executed_player.role.capitalize())
                role_emoji = ROLE_EMOJIS.get(executed_player.role, "?") # Эмодзи для роли

                execution_message = f"По результатам голосования, жители решили казнить <a href='tg://user?id={executed_player.user_id}'><b>{executed_player.full_name}</b></a>.\n" \
                                    f"Его роль была: {role_emoji} {killed_player_role_ru}. {PHASE_EMOJIS['death']}"

                await send_death_notification_and_farewell_prompt(executed_player.user_id, executed_player.game_id,
                                                                  executed_player.role, executed_player.full_name)
                session.commit() # Коммит после смерти, чтобы check_win_condition видел актуальное состояние
                if await check_win_condition(game.id, session):
                    game_ended_after_lynch = True
                else:
                    execution_message = f"Жители решили помиловать <a href='tg://user?id={executed_player.user_id}'><b>{executed_player.full_name}</b></a>. Он остается жить! {RESULT_EMOJIS['saved']}"
                    session.commit() # Коммит, чтобы сохранить статус игры

            await bot.send_message(
                chat_id=game.chat_id,
                text=f"{PHASE_EMOJIS['lynch_vote']} <b>Итоги голосования за казнь:</b>\n"
                     f"{FACTION_EMOJIS['mafia']} За казнь: {lynch_likes}\n"
                     f"{FACTION_EMOJIS['town']} Против казни: {lynch_dislikes}\n\n"
                     f"{execution_message}",
                parse_mode=ParseMode.HTML
            )

            if game.lynch_message_id:
                try:
                    await bot.edit_message_reply_markup(
                        chat_id=game.chat_id,
                        message_id=game.lynch_message_id,
                        reply_markup=None
                    )
                    logging.info(f"Removed lynch voting buttons from chat {game.chat_id}, message {game.lynch_message_id}.")
                except (TelegramBadRequest, TelegramForbiddenError) as e:
                    logging.warning(f"Не удалось удалить/отредактировать сообщение с кнопками линчевания {game.lynch_message_id}: {e}")
                except Exception as e:
                    logging.error(f"Неизвестная ошибка при удалении кнопок линчевания: {e}")


            if not game_ended_after_lynch:
                await prepare_for_night_phase(game.id, session)

        except Exception as e:
            logging.error(f"Ошибка в end_lynch_voting_phase для game_id {game_id}: {e}", exc_info=True)
            session.rollback()


async def prepare_for_night_phase(game_id: int, session):
    """Общая функция для перехода к ночной фазе."""
    game = session.get(Game, game_id)
    if not game:
        logging.warning(f"prepare_for_night_phase: Game {game_id} not found.")
        return

    # Сброс флагов голосования для всех живых игроков перед новой ночью/днем
    players_to_reset_votes = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()
    for p in players_to_reset_votes:
        p.voted_for_player_id = None
        session.add(p)
    session.commit()
    logging.info(f"Players' votes reset for game {game.id} before new night.")

    game.current_day += 1 # День увеличивается перед ночью
    game.phase = 'night'
    game.phase_end_time = datetime.datetime.now() + datetime.timedelta(seconds=PHASE_DURATIONS['night'])
    game.voted_for_player_id = None
    game.lynch_vote_likes = 0
    game.lynch_vote_dislikes = 0
    game.lynch_message_id = None
    game.lynch_voters = ""

    session.add(game)
    session.commit()
    logging.info(f"Game {game_id} transitioned to 'night' phase {game.current_day}.")

    await start_night_phase(game.id)

    scheduler.add_job(end_night_phase_processing, 'date', run_date=game.phase_end_time, args=[game_id], id=f"end_night_processing_game_{game_id}")
    logging.info(f"Scheduled end_night_phase_processing for game {game_id} at {game.phase_end_time}")


async def start_night_phase(game_id: int):
    """Отправляет кнопки ночных действий игрокам с ролями в ЛС."""
    with Session() as session:
        try:
            game = session.get(Game, game_id)
            if not game or game.status != 'playing' or game.phase != 'night':
                logging.warning(f"start_night_phase: Game {game_id} not in night phase or not playing.")
                return

            players_alive = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()

            for p in players_alive:
                p.night_action_target_id = None
                session.add(p)
                # Очистка FSM для всех, кроме мафии/донов, которые получают FSM далее
                if p.role not in ['mafia', 'don']:
                    state = dp.fsm.get_context(bot=bot, chat_id=p.user_id, user_id=p.user_id)
                    await state.clear()
            session.commit()
            logging.info(f"Night actions reset and FSM cleared for non-mafia/don players in game {game.id}.")

            for player in players_alive:
                target_buttons = []
                keyboard_message = ""

                # Мафия/Дон: отправка кнопок и перевод в FSM для чата
                if player.role == 'mafia' or player.role == 'don':
                    keyboard_message = f"Ночь! {ROLE_EMOJIS[player.role]} Выберите, кого Мафия убьет этой ночью:"
                    # Мафия не может убить себя или других мафиози
                    possible_targets = [p for p in players_alive if p.role not in ['mafia', 'don'] and p.id != player.id]

                    for target in possible_targets:
                        target_buttons.append(
                            InlineKeyboardButton(text=target.full_name, callback_data=f"mafia_kill_{game.id}_{target.id}"))
                    
                    # --- Перевод в FSM для внутрифракционного чата ---
                    state = dp.fsm.get_context(bot=bot, chat_id=player.user_id, user_id=player.user_id)
                    await state.set_state(GameState.waiting_for_faction_message)
                    await state.update_data(game_id=game.id, player_id=player.id, player_full_name=player.full_name, player_role=player.role)
                    try:
                        await bot.send_message(
                            chat_id=player.user_id,
                            text=f"Вы находитесь в закрытом чате Мафии. {FACTION_EMOJIS['mafia']} "
                                 f"Ваши сообщения, отправленные сюда, будут видны только союзникам."
                        )
                    except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован игроком {player.full_name}. Не удалось отправить сообщение о чате фракции.")
                    except Exception as e:
                        logging.error(f"Ошибка при отправке сообщения о чате фракции игроку {player.full_name}: {e}")

                # Доктор
                elif player.role == 'doctor':
                    keyboard_message = f"Ночь! {ROLE_EMOJIS['doctor']} Выберите, кого вы вылечите этой ночью:"
                    possible_targets = players_alive
                    for target in possible_targets:
                        target_buttons.append(
                            InlineKeyboardButton(text=target.full_name, callback_data=f"doctor_heal_{game.id}_{target.id}"))

                # Комиссар
                elif player.role == 'commissioner':
                    keyboard_message = f"Ночь! {ROLE_EMOJIS['commissioner']} Выберите, кого вы проверите этой ночью:"
                    possible_targets = [p for p in players_alive if p.id != player.id]
                    for target in possible_targets:
                        target_buttons.append(
                            InlineKeyboardButton(text=target.full_name, callback_data=f"com_check_{game.id}_{target.id}"))

                # Маньяк
                elif player.role == 'maniac':
                    keyboard_message = f"Ночь! {ROLE_EMOJIS['maniac']} Выберите, кого вы убьете этой ночью:"
                    possible_targets = [p for p in players_alive if p.id != player.id]
                    for target in possible_targets:
                        target_buttons.append(
                            InlineKeyboardButton(text=target.full_name, callback_data=f"maniac_kill_{game.id}_{target.id}"))

                if target_buttons:
                    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        target_buttons[i:i + 2] for i in range(0, len(target_buttons), 2)
                    ])
                    try:
                        await bot.send_message(
                            chat_id=player.user_id,
                            text=keyboard_message,
                            reply_markup=inline_keyboard
                        )
                        logging.info(f"Sent night action buttons to player {player.full_name} ({player.role}) for game {game_id}.")
                    except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован игроком {player.full_name} ({player.user_id}) в игре {game.id}. Не удалось отправить ночные кнопки.")
                        await bot.send_message(game.chat_id,
                                               f"ВНИМАНИЕ: Не удалось отправить ночные действия игроку <a href='tg://user?id={player.user_id}'>{player.full_name}</a>. Пожалуйста, попросите его написать боту /start в ЛС, чтобы он мог принимать сообщения.",
                                               parse_mode=ParseMode.HTML)
                    except Exception as e:
                        logging.error(f"Не удалось отправить ночные кнопки игроку {player.full_name} ({player.user_id}) для game {game.id}: {e}", exc_info=True)
                        await bot.send_message(game.chat_id,
                                               f"ВНИМАНИЕ: Не удалось отправить ночные действия игроку <a href='tg://user?id={player.user_id}'>{player.full_name}</a>. Пожалуйста, попросите его написать боту /start в ЛС, чтобы он мог принимать сообщения.",
                                               parse_mode=ParseMode.HTML)

            caption_text = (
                f"<b>Наступает ночь!</b> {PHASE_EMOJIS['night']}\n"
                f"На улицы города выходят лишь самые отважные и бесстрашные. "
                f"Утром попробуем сосчитать их головы...\n"
                f"У вас есть {PHASE_DURATIONS['night']} секунд на ночные действия. "
                f"Проверьте свои личные сообщения от бота, если у вас есть особая роль."
            )

            try:
                await bot.send_animation(
                    chat_id=game.chat_id,
                    animation=NIGHT_ANIMATION_FILE_ID,
                    caption=caption_text,
                    parse_mode=ParseMode.HTML
                )
                logging.info(f"Sent night animation to chat {game.chat_id} for game {game_id}.")
            except (TelegramBadRequest, TelegramForbiddenError) as e:
                logging.error(f"Не удалось отправить GIF для ночной фазы в чат {game.chat_id} (вероятно, некорректный file_id или бот заблокирован): {e}", exc_info=True)
                await bot.send_message(
                    chat_id=game.chat_id,
                    text=caption_text,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logging.error(f"Не удалось отправить GIF для ночной фазы в чат {game.chat_id}: {e}", exc_info=True)
                await bot.send_message(
                    chat_id=game.chat_id,
                    text=caption_text,
                    parse_mode=ParseMode.HTML
                )

        except Exception as e:
            logging.error(f"Ошибка в start_night_phase (отправка кнопок) для game_id {game_id}: {e}", exc_info=True)
            session.rollback()

async def end_night_phase_processing(game_id: int):
    """Завершает ночную фазу, обрабатывает действия ролей и начинает новый день."""
    with Session() as session:
        try:
            game = session.get(Game, game_id)
            if not game or game.status != 'playing' or game.phase != 'night':
                logging.warning(f"end_night_phase_processing: Game {game_id} not in night phase or not playing (status={game.status}, phase={game.phase}).")
                return

            players_alive = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()

            mafia_target_id = None
            doctor_target_id = None
            commissioner_target_id = None
            maniac_target_id = None

            mafia_players = session.query(Player).filter_by(game_id=game.id, is_alive=True, role='mafia').all()
            don_player = session.query(Player).filter_by(game_id=game.id, is_alive=True, role='don').first()

            # ЛОГИКА ГОЛОСОВАНИЯ МАФИИ: Приоритет Дону, затем большинство
            if don_player and don_player.night_action_target_id:
                mafia_target_id = don_player.night_action_target_id
                logging.info(f"Don's vote {mafia_target_id} is decisive for mafia in game {game.id}.")
            else:
                mafia_votes_count = {}
                all_mafia_active = mafia_players + ([don_player] if don_player else [])
                for mafia_p in all_mafia_active:
                    if mafia_p.night_action_target_id:
                        target_id = mafia_p.night_action_target_id
                        mafia_votes_count[target_id] = mafia_votes_count.get(target_id, 0) + 1
                
                if mafia_votes_count:
                    max_votes_mafia = 0
                    mafia_targets_with_max_votes = []
                    for target_id, count in mafia_votes_count.items():
                        if count > max_votes_mafia:
                            max_votes_mafia = count
                            mafia_targets_with_max_votes = [target_id]
                        elif count == max_votes_mafia:
                            mafia_targets_with_max_votes.append(target_id)

                    if len(mafia_targets_with_max_votes) == 1:
                        mafia_target_id = mafia_targets_with_max_votes[0]
                    elif len(mafia_targets_with_max_votes) > 1:
                        mafia_target_id = random.choice(mafia_targets_with_max_votes)
                        logging.info(f"Mafia targets tied in game {game.id}. Randomly chose {mafia_target_id}.")
                        
            doctor_player = session.query(Player).filter_by(game_id=game.id, is_alive=True, role='doctor').first()
            if doctor_player and doctor_player.night_action_target_id:
                doctor_target_id = doctor_player.night_action_target_id

            commissioner_player = session.query(Player).filter_by(game_id=game.id, is_alive=True, role='commissioner').first()
            if commissioner_player and commissioner_player.night_action_target_id:
                commissioner_target_id = commissioner_player.night_action_target_id

            maniac_player = session.query(Player).filter_by(game_id=game.id, is_alive=True, role='maniac').first()
            if maniac_player and maniac_player.night_action_target_id:
                maniac_target_id = maniac_player.night_action_target_id

            killed_by_mafia_player = session.get(Player, mafia_target_id) if mafia_target_id else None
            healed_by_doctor_player = session.get(Player, doctor_target_id) if doctor_target_id else None
            killed_by_maniac_player = session.get(Player, maniac_target_id) if maniac_target_id else None

            night_kill_message = f"Этой ночью никто не погиб. {RESULT_EMOJIS['missed']}\n"
            players_to_kill_ids = set()
            if killed_by_mafia_player:
                players_to_kill_ids.add(killed_by_mafia_player.id)
            if killed_by_maniac_player:
                players_to_kill_ids.add(killed_by_maniac_player.id)

            killed_players_names = []
            
            doctor_target_was_attacked = False

            for player_id_to_kill in list(players_to_kill_ids):
                player_obj = session.get(Player, player_id_to_kill)
                if not player_obj or not player_obj.is_alive:
                    continue

                if healed_by_doctor_player and player_obj.id == healed_by_doctor_player.id:
                    doctor_target_was_attacked = True
                    night_kill_message = f"Этой ночью на <a href='tg://user?id={player_obj.user_id}'><b>{player_obj.full_name}</b></a> было совершено нападение, но {ROLE_EMOJIS['doctor']} Доктор его спас! {RESULT_EMOJIS['saved']}"

                    try:
                        await bot.send_message(chat_id=player_obj.user_id,text=f"{ROLE_EMOJIS['doctor']} Вы почувствовали легкое недомогание, но утром почувствовали себя лучше. Вас посетил {ROLE_EMOJIS['doctor']} Доктор! "
                                               f"Этой ночью на вас было совершено нападение, но вы выжили!"
                                               )
                    except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован спасенным игроком {player_obj.full_name}: не удалось уведомить.")
                    except Exception as e:
                        logging.warning(f"Не удалось уведомить спасенного игрока {player_obj.full_name}: {e}")

                    if doctor_player:
                        try:
                            await bot.send_message(
                                chat_id=doctor_player.user_id,
                                text=f"{ROLE_EMOJIS['doctor']} Вы исцелили <a href='tg://user?id={player_obj.user_id}'><b>{player_obj.full_name}</b></a>! Вы спасли ему жизнь! {RESULT_EMOJIS['saved']}"
                            )
                        except TelegramForbiddenError:
                            logging.warning(f"Бот заблокирован доктором {doctor_player.full_name}: не удалось уведомить о спасении.")
                        except Exception as e:
                            logging.warning(f"Не удалось уведомить доктора о спасении: {e}")

                else:
                    player_obj.is_alive = False
                    player_obj.total_deaths = (player_obj.total_deaths or 0) + 1 # Увеличиваем счетчик смертей
                    session.add(player_obj)
                    killed_players_names.append(
                        f"<a href='tg://user?id={player_obj.user_id}'>{player_obj.full_name}</a> "
                        f"({ROLE_EMOJIS.get(player_obj.role, '?')} {ROLE_NAMES_RU.get(player_obj.role, player_obj.role.capitalize())})"
                    )

                    if player_id_to_kill == mafia_target_id:
                        for mafia_p in mafia_players + ([don_player] if don_player else []):
                            if mafia_p and mafia_p.id != player_id_to_kill:
                                mafia_p.total_kills = (mafia_p.total_kills or 0) + 1
                                session.add(mafia_p)
                    
                    if player_id_to_kill == maniac_target_id and maniac_player:
                        maniac_player.total_kills = (maniac_player.total_kills or 0) + 1
                        session.add(maniac_player)
                    
                    session.commit()
                    logging.info(f"Player {player_obj.full_name} was killed in game {game.id}.")

                    await send_death_notification_and_farewell_prompt(player_obj.user_id,
                                                                      player_obj.game_id, player_obj.role,
                                                                      player_obj.full_name)
            
            # --- Уведомления фракциям о результате их действий и очистка FSM ---
            all_mafia = mafia_players + ([don_player] if don_player else [])
            if all_mafia: # Только если есть живые мафиози
                mafia_result_message = ""
                if mafia_target_id:
                    target_name_mafia = session.get(Player, mafia_target_id).full_name if session.get(Player, mafia_target_id) else "неизвестного"
                    killed_role_text_mafia = ""
                    if mafia_target_id in players_to_kill_ids:
                        killed_player_obj_mafia = session.get(Player, mafia_target_id)
                        killed_role_text_mafia = f" ({ROLE_NAMES_RU.get(killed_player_obj_mafia.role, killed_player_obj_mafia.role.capitalize())})"
                        if healed_by_doctor_player and mafia_target_id == healed_by_doctor_player.id:
                            mafia_result_message = f"{FACTION_EMOJIS['mafia']} Вы пытались убить <b>{target_name_mafia}</b>, но он был спасен Доктором! {RESULT_EMOJIS['saved']}"
                        else:
                            mafia_result_message = f"{FACTION_EMOJIS['mafia']} Вы успешно убили <b>{target_name_mafia}</b>{killed_role_text_mafia}! {RESULT_EMOJIS['success']}"
                    elif healed_by_doctor_player and mafia_target_id == healed_by_doctor_player.id:
                        mafia_result_message = f"{FACTION_EMOJIS['mafia']} Вы пытались убить <b>{target_name_mafia}</b>, но он был спасен Доктором! {RESULT_EMOJIS['saved']}"
                    else: # Если цель была выбрана, но не убита и не спасена (например, целью был мертвый игрок)
                        mafia_result_message = f"{FACTION_EMOJIS['mafia']} Вы выбрали <b>{target_name_mafia}</b>, но ничего не произошло. Возможно, цель уже мертва. {RESULT_EMOJIS['missed']}"
                else:
                    mafia_result_message = f"{FACTION_EMOJIS['mafia']} Вы никого не выбрали для убийства этой ночью. Жертва упущена! {RESULT_EMOJIS['missed']}"
                
                for mafia_p in all_mafia:
                    try:
                        await bot.send_message(chat_id=mafia_p.user_id, text=mafia_result_message, parse_mode=ParseMode.HTML)
                        state = dp.fsm.get_context(bot=bot, chat_id=mafia_p.user_id, user_id=mafia_p.user_id)
                        await state.clear() # Очищаем FSM после получения итогов ночи
                    except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован мафиози {mafia_p.full_name}: не удалось уведомить об убийстве.")
                    except Exception as e:
                        logging.warning(f"Не удалось уведомить мафиози {mafia_p.full_name} об убийстве: {e}")

            if maniac_player:
                maniac_result_message = ""
                if maniac_target_id:
                    target_name_maniac = session.get(Player, maniac_target_id).full_name if session.get(Player, maniac_target_id) else "неизвестного"
                    killed_role_text_maniac = ""
                    if maniac_target_id in players_to_kill_ids:
                        killed_player_obj_maniac = session.get(Player, maniac_target_id)
                        killed_role_text_maniac = f" ({ROLE_NAMES_RU.get(killed_player_obj_maniac.role, killed_player_obj_maniac.role.capitalize())})"

                    if healed_by_doctor_player and maniac_target_id == healed_by_doctor_player.id:
                        maniac_result_message = f"{ROLE_EMOJIS['maniac']} Вы пытались убить <b>{target_name_maniac}</b>, но он был спасен Доктором! {RESULT_EMOJIS['saved']}"
                    else:
                        maniac_result_message = f"{ROLE_EMOJIS['maniac']} Вы успешно убили <b>{target_name_maniac}</b>{killed_role_text_maniac}! {RESULT_EMOJIS['success']}"
                else:
                    maniac_result_message = f"{ROLE_EMOJIS['maniac']} Вы никого не выбрали для убийства этой ночью. Жертва упущена! {RESULT_EMOJIS['missed']}"

                try:
                    await bot.send_message(chat_id=maniac_player.user_id, text=maniac_result_message, parse_mode=ParseMode.HTML)
                    state = dp.fsm.get_context(bot=bot, chat_id=maniac_player.user_id, user_id=maniac_player.user_id)
                    await state.clear() # Очищаем FSM после получения итогов ночи
                except TelegramForbiddenError:
                    logging.warning(f"Бот заблокирован маньяком {maniac_player.full_name}: не удалось уведомить.")
                except Exception as e:
                    logging.warning(f"Не удалось уведомить Маньяка {maniac_player.full_name} об убийстве: {e}")

            if doctor_player:
                doctor_result_message = ""
                if doctor_target_id:
                    target_of_doctor = session.get(Player, doctor_target_id)
                    if target_of_doctor:
                        if not doctor_target_was_attacked:
                            doctor_result_message = f"{ROLE_EMOJIS['doctor']} Вы вылечили <a href='tg://user?id={target_of_doctor.user_id}'><b>{target_of_doctor.full_name}</b></a>. " \
                                                    f"К счастью, этой ночью на него никто не нападал. Ваши бинты и скальпель не пригодились. {RESULT_EMOJIS['missed']}"
                        else:
                            doctor_result_message = f"{ROLE_EMOJIS['doctor']} Вы исцелили <a href='tg://user?id={target_of_doctor.user_id}'><b>{target_of_doctor.full_name}</b></a>! Вы спасли ему жизнь! {RESULT_EMOJIS['success']}"
                else:
                    doctor_result_message = f"{ROLE_EMOJIS['doctor']} Вы никого не выбрали для лечения этой ночью. Ваши бинты и скальпель остались нетронутыми. {RESULT_EMOJIS['missed']}"
                
                try:
                    await bot.send_message(chat_id=doctor_player.user_id, text=doctor_result_message, parse_mode=ParseMode.HTML)
                    state = dp.fsm.get_context(bot=bot, chat_id=doctor_player.user_id, user_id=doctor_player.user_id)
                    await state.clear()
                    # Очищаем FSM после получения итогов ночи
                except TelegramForbiddenError:
                    logging.warning(f"Бот заблокирован доктором {doctor_player.full_name}: не удалось уведомить.")
                except Exception as e:
                    logging.warning(f"Не удалось уведомить доктора {doctor_player.full_name} об отсутствии действия: {e}")

            if commissioner_player and not commissioner_target_id:
                try:
                    await bot.send_message(chat_id=commissioner_player.user_id,
                        text=f"{ROLE_EMOJIS['commissioner']} Вы никого не проверили этой ночью. Ваша бдительность была напрасной. {RESULT_EMOJIS['missed']}")
                    state = dp.fsm.get_context(bot=bot, chat_id=commissioner_player.user_id, user_id=commissioner_player.user_id)
                    await state.clear() # Очищаем FSM после получения итогов ночи
                except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован комиссаром {commissioner_player.full_name}: не удалось уведомить об отсутствии действия.")
                except Exception as e:
                    logging.warning(f"Не удалось уведомить комиссара {commissioner_player.full_name} об отсутствии действия: {e}")

            # Формируем сообщение о смерти/спасении для общего чата
            if len(killed_players_names) > 0:
                if len(killed_players_names) == 1:
                    night_kill_message = f"Этой ночью погиб {killed_players_names[0]}. {PHASE_EMOJIS['death']}"
                else:
                    night_kill_message = f"Этой ночью погибли следующие игроки: {', '.join(killed_players_names)}. {PHASE_EMOJIS['death']}"
            elif not players_to_kill_ids and not doctor_target_was_attacked:
                night_kill_message = f"Этой ночью никто не погиб. {RESULT_EMOJIS['missed']}"
            
            # --- Отправка дневной GIF и улучшенной сводки ---
            await bot.send_message(
                chat_id=game.chat_id,
                text=f"Ночь завершена. Утро наступило! {PHASE_EMOJIS['day']} \n"
                     f"{night_kill_message}",
                parse_mode=ParseMode.HTML
            )
            logging.info(f"Night phase ended for game {game.id}. Kill message sent to chat.")

            if await check_win_condition(game.id, session):
                return

            game.phase = 'day'
            game.phase_end_time = datetime.datetime.now() + datetime.timedelta(seconds=PHASE_DURATIONS['day'])
            session.add(game)
            session.commit()
            logging.info(f"Game {game.id} transitioned to 'day' phase {game.current_day}.")

            caption_text = (
                f"<b>Наступает День {game.current_day}!</b> {PHASE_EMOJIS['day']}\n"
                f"Солнце выходит, и иссушает кровь, пролитую ночью на тротуарах......\n"
                f"Сейчас самое время обсудить последствия ночи, понять причины и последствия......\n"
                f"Эта фаза продлится {PHASE_DURATIONS['day']} секунд."
            )
            
            try:
                await bot.send_animation(
                    chat_id=game.chat_id,
                    animation=DAY_ANIMATION_FILE_ID,
                    caption=caption_text,parse_mode=ParseMode.HTML
                )
                logging.info(f"Sent day animation to chat {game.chat_id} for game {game.id}.")
            except (TelegramBadRequest, TelegramForbiddenError) as e:
                logging.error(f"Не удалось отправить GIF для дневной фазы в чат {game.chat_id}: {e}", exc_info=True)
                # Если GIF не отправилась, продолжим отправлять текстовое сообщение
            except Exception as e:
                logging.error(f"Неизвестная ошибка при отправке GIF для дневной фазы: {e}", exc_info=True)
                # Если GIF не отправилась, продолжим отправлять текстовое сообщение

            players_alive_day = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()
            players_dead_day = session.query(Player).filter_by(game_id=game.id, is_alive=False).all()

            day_info_message_parts = []

            # 1. Список живых игроков
            if players_alive_day:
                day_info_message_parts.append("\n<b>? Живые игроки:</b>")
                for i, p in enumerate(players_alive_day, 1):
                    gender_emoji = GENDER_EMOJIS.get(p.gender, "?")
                    player_link = f"<a href='tg://user?id={p.user_id}'>{p.full_name}</a>"
                    username_part = f" | @{p.username}" if p.username else ""
                    day_info_message_parts.append(f"{i}. {gender_emoji} {player_link}{username_part}")

                # 2. Детальная сводка по ролям живых игроков
                role_counts_alive = {}
                for p in players_alive_day:
                    role_name_ru = ROLE_NAMES_RU.get(p.role, p.role.capitalize())
                    faction_name_ru = ""
                    if p.role in ['mafia', 'don']: faction_name_ru = "Мафия"
                    elif p.role == 'maniac': faction_name_ru = "Одиночка"
                    else: faction_name_ru = "Мирные жители"

                    role_counts_alive.setdefault(faction_name_ru, {'total': 0, 'details': {}})
                    role_counts_alive[faction_name_ru]['total'] += 1
                    role_counts_alive[faction_name_ru]['details'].setdefault(p.role, {'count': 0, 'name': role_name_ru, 'emoji': ROLE_EMOJIS.get(p.role, "?")})
                    role_counts_alive[faction_name_ru]['details'][p.role]['count'] += 1
                
                day_info_message_parts.append("\n<b>Из них:</b>")
                
                faction_order = {'Мирные жители': 1, 'Мафия': 2, 'Одиночка': 3}
                sorted_factions = sorted(role_counts_alive.items(), key=lambda item: faction_order.get(item[0], 99))

                for faction_name_ru, faction_data in sorted_factions:
                    faction_emoji = FACTION_EMOJIS.get(faction_name_ru.lower().replace(' ', ''), "?")
                    day_info_message_parts.append(f"{faction_emoji} <b>{faction_name_ru} - {faction_data['total']}</b>")
                    
                    sorted_roles_in_faction = sorted(faction_data['details'].items(), key=lambda item: item[1]['name'])
                    for role_type, role_detail in sorted_roles_in_faction:
                        if role_detail['count'] > 0:
                            if role_detail['count'] == 1 and faction_name_ru == 'Мирные жители' and role_type != 'civilian':
                                day_info_message_parts.append(f"    {role_detail['emoji']} {role_detail['name']}")
                            elif role_detail['count'] == 1 and faction_name_ru == 'Мафия' and role_type != 'mafia':
                                day_info_message_parts.append(f"    {role_detail['emoji']} {role_detail['name']}")
                            else:
                                day_info_message_parts.append(f"    {role_detail['emoji']} {role_detail['name']} - {role_detail['count']}")

                day_info_message_parts.append(f"\n? Всего: {len(players_alive_day)} человек")
            
            # 3. Список мертвых игроков
            if players_dead_day:
                day_info_message_parts.append(f"\n<b>{PHASE_EMOJIS['death']} Мертвые игроки:</b>\n") # Добавлено: строка с заголовком мертвых игроков
                
                role_counts_dead = {}
                for p in players_dead_day:
                    role_name_ru = ROLE_NAMES_RU.get(p.role, p.role.capitalize())
                    faction_name_ru = ""
                    if p.role in ['mafia', 'don']: faction_name_ru = "Мафия"
                    elif p.role == 'maniac': faction_name_ru = "Одиночка"
                    else: faction_name_ru = "Мирные жители"
                    
                    role_counts_dead.setdefault(faction_name_ru, {'total': 0, 'details': {}})
                    role_counts_dead[faction_name_ru]['total'] += 1
                    role_counts_dead[faction_name_ru]['details'].setdefault(p.role, {'count': 0, 'name': role_name_ru, 'emoji': ROLE_EMOJIS.get(p.role, "?")})
                    role_counts_dead[faction_name_ru]['details'][p.role]['count'] += 1

                sorted_factions_dead = sorted(role_counts_dead.items(), key=lambda item: faction_order.get(item[0], 99))

                for faction_name_ru, faction_data in sorted_factions_dead:
                    faction_emoji = FACTION_EMOJIS.get(faction_name_ru.lower().replace(' ', ''), "?")
                    day_info_message_parts.append(f"{faction_emoji}<b>{faction_name_ru} - {faction_data['total']}</b>")
                    
                    sorted_roles_in_faction = sorted(faction_data['details'].items(), key=lambda item: item[1]['name'])
                    for role_type, role_detail in sorted_roles_in_faction:
                        if role_detail['count'] > 0:
                            if role_detail['count'] == 1:
                                day_info_message_parts.append(f"    {PHASE_EMOJIS['death']} {role_detail['emoji']} {role_detail['name']}")
                            else:
                                day_info_message_parts.append(f"    {PHASE_EMOJIS['death']} {role_detail['emoji']} {role_detail['name']} - {role_detail['count']}")

            inline_keyboard_to_bot = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="? Перейти к боту", url=f"https://t.me/{bot_self_info.username if bot_self_info else 'your_bot_username'}?start=game_{game.id}")]])

            await bot.send_message(
                chat_id=game.chat_id,
                text="\n".join(day_info_message_parts),
                reply_markup=inline_keyboard_to_bot,
                parse_mode=ParseMode.HTML
            )

            scheduler.add_job(end_day_phase, 'date', run_date=game.phase_end_time, args=[game.id],
                              id=f"end_day_game_{game.id}")
            logging.info(f"Scheduled end_day_phase for game {game.id} at {game.phase_end_time}.")

        except Exception as e:
            logging.error(f"Ошибка в end_night_phase_processing для game_id {game_id}: {e}", exc_info=True)
            session.rollback()
                        

async def check_win_condition(game_id: int, session) -> bool:
    """
    Проверяет, выполнено ли условие победы для какой-либо из сторон.
    Возвращает True, если игра завершена, False в противном случае.
    """
    logging.info(f"Checking win condition for game {game_id}")
    game = session.get(Game, game_id)
    if not game or game.status != 'playing':
        logging.info(f"Game {game.id} not playing, skipping win check.")
        return False

    players_alive = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()
    
    num_mafia_alive = len([p for p in players_alive if p.role in ['mafia', 'don']])
    num_town_alive = len([p for p in players_alive if p.role in ['civilian', 'doctor', 'commissioner']])
    num_maniac_alive = len([p for p in players_alive if p.role == 'maniac'])

    num_total_alive = num_mafia_alive + num_town_alive + num_maniac_alive

    logging.info(f"Game {game.id}: Mafia alive: {num_mafia_alive}, Town alive: {num_town_alive}, Maniac alive: {num_maniac_alive}, Total alive: {num_total_alive}")
    winner_message = ""
    game_over = False
    winning_faction_players = []

    # Условие победы Маньяка
    if num_maniac_alive > 0 and num_maniac_alive >= num_total_alive - num_maniac_alive:
        winner_message = f"{ROLE_EMOJIS['maniac']} Маньяк победил!"
        game_over = True
        winning_faction_players = session.query(Player).filter_by(game_id=game.id, role='maniac').all()
    # Условие победы Мафии
    elif num_mafia_alive > 0 and num_mafia_alive >= (num_town_alive + num_maniac_alive):
        winner_message = f"{FACTION_EMOJIS['mafia']} Мафия победила!"
        game_over = True
        winning_faction_players = session.query(Player).filter(Player.game_id == game.id, Player.role.in_(['mafia', 'don'])).all()
    # Условие победы Мирных
    elif num_mafia_alive == 0 and num_maniac_alive == 0 and num_town_alive > 0:
        winner_message = f"{FACTION_EMOJIS['town']} Мирные жители победили!"
        game_over = True
        winning_faction_players = session.query(Player).filter(Player.game_id == game.id, Player.role.in_(['civilian', 'doctor', 'commissioner'])).all()
    # Ничья (все мертвы)
    elif num_total_alive == 0:
        winner_message = f"{RESULT_EMOJIS['missed']} Никто не выжил. Ничья."
        game_over = True
    
    if game_over:
        logging.info(f"Game {game.id} IS OVER! Winner: {winner_message}")
        game_chat_id = game.chat_id

        all_players_in_finished_game = session.query(Player).filter_by(game_id=game_id).all()

        # Сначала обновляем статус игры в базе данных
        game.status = 'finished'
        session.add(game)
        session.commit()
        logging.info(f"Game {game.id} status committed as 'finished'.")
        # --- ОБНОВЛЕНИЕ СТАТИСТИКИ ГРУППЫ (ГОРОДА) И ПРИМЕНЕНИЕ БОНУСОВ ---
        group = session.query(Group).filter_by(chat_id=game.chat_id).first()
        if group:
            group.total_games_played += 1
            group.experience += GROUP_EXP_FOR_GAME_END # Базовый опыт за завершение игры
            group.experience += len(all_players_in_finished_game) * GROUP_EXP_PER_PLAYER_BONUS # Бонус за каждого игрока

            # Проверяем и применяем бонусы при повышении уровня группы
            while group.experience >= (BASE_GROUP_EXP_FOR_LEVEL_UP + (group.level * GROUP_LEVEL_UP_EXP_INCREMENT)):
                group.level += 1
                # Опыт для нового уровня рассчитывается на основе *предыдущего* уровня.
                # Поэтому вычитаем опыт, необходимый для *предыдущего* уровня.
                # Если уровень был 1, опыт для 2-го уровня - BASE + 1*INCREMENT.
                # Тогда из текущего опыта вычитается BASE + (group.level - 1)*INCREMENT
                exp_needed_for_previous_level_up = BASE_GROUP_EXP_FOR_LEVEL_UP + ((group.level - 1) * GROUP_LEVEL_UP_EXP_INCREMENT)
                group.experience -= exp_needed_for_previous_level_up
                
                # Применяем бонусы нового уровня
                level_bonus_key = min(group.level, max(GROUP_LEVEL_BONUSES.keys())) # Берем ключ из конфига, не превышающий текущий уровень
                level_bonus = GROUP_LEVEL_BONUSES.get(level_bonus_key, GROUP_LEVEL_BONUSES.get(max(GROUP_LEVEL_BONUSES.keys()))) # берем максимальный, если уровень выше последнего в конфиге
                group.bonus_exp_percent = level_bonus['exp_percent']
                group.bonus_dollars_percent = level_bonus['dollars_percent']
                group.bonus_item_chance = level_bonus['item_chance']

                await bot.send_message(group.chat_id,
                                       f"Поздравляем! {FACTION_EMOJIS['town']} Уровень Города <b>{group.name}</b> повышен до <b>{group.level}</b>!\n"
                                       f"Теперь все игроки в этой группе получают: +{group.bonus_exp_percent*100:.0f}% к опыту, +{group.bonus_dollars_percent*100:.0f}% к Долларам.",
                                       parse_mode=ParseMode.HTML)
            session.add(group)
            logging.info(f"Group {group.name} (ID: {group.id}) stats updated for game {game.id}.")
        # --- КОНЕЦ ОБНОВЛЕНИЯ СТАТИСТИКИ ГРУППЫ ---

        game_start_time = game.start_actual_time if game.start_actual_time else game.created_at
        game_end_time = datetime.datetime.now()
        duration = game_end_time - game_start_time
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_parts = []
        if hours > 0: duration_parts.append(f"{hours} ч")
        if minutes > 0: duration_parts.append(f"{minutes} мин")
        if seconds > 0 or not duration_parts: duration_parts.append(f"{seconds} сек")
        duration_text = " ".join(duration_parts)

         
        for player_game_instance in all_players_in_finished_game: # player_game_instance - это запись игрока В КОНКРЕТНОЙ ИГРЕ
            # Убедимся, что у игрока есть ГЛОБАЛЬНЫЙ ПРОФИЛЬ
            global_player_profile = ensure_player_profile_exists(session, player_game_instance.user_id, player_game_instance.username, player_game_instance.full_name)
            
            # Применяем бонусы группы, если игрок играл в группе с бонусами
            group_bonus_exp_multiplier = 1.0
            group_bonus_dollars_multiplier = 1.0
            
            player_last_played_group = session.get(Group, player_game_instance.last_played_group_id)
            if player_last_played_group:
                group_bonus_exp_multiplier += player_last_played_group.bonus_exp_percent
                group_bonus_dollars_multiplier += player_last_played_group.bonus_dollars_percent
                logging.debug(f"Player {player_game_instance.full_name} gets group bonuses: exp_mult={group_bonus_exp_multiplier}, dollars_mult={group_bonus_dollars_multiplier}")


            player_is_mafia_faction = player_game_instance.role in ['mafia', 'don']
            player_is_maniac_faction = player_game_instance.role == 'maniac'
            player_is_town_faction = player_game_instance.role in ['civilian', 'doctor', 'commissioner']

            is_winning_player = False
            # Победителями считаются живые члены победившей фракции.
            # Мертвые члены победившей фракции получают награду за участие + бонус за победу.
            # Живые/мертвые члены проигравшей фракции получают награду за участие + штраф/утешительный приз.
            
            if (player_is_mafia_faction and 'Мафия победила' in winner_message) or \
               (player_is_maniac_faction and 'Маньяк победил' in winner_message) or \
               (player_is_town_faction and 'Мирные жители победили' in winner_message):
                is_winning_player = True
            
            # Базовые награды за участие
            exp_gained = EXP_FOR_PARTICIPATION
            dollars_gained = DOLLARS_FOR_PARTICIPATION

            if is_winning_player:
                global_player_profile.total_wins = (global_player_profile.total_wins or 0) + 1
                exp_gained += EXP_FOR_WIN
                dollars_gained += DOLLARS_FOR_WIN
            else:
                dollars_gained += DOLLARS_FOR_LOSS # Утешительный приз проигравшим/мертвым
            
            # Применяем множители от группы
            global_player_profile.experience += int(exp_gained * group_bonus_exp_multiplier)
            global_player_profile.dollars += int(dollars_gained * group_bonus_dollars_multiplier)

            # Обновляем last_played_group_id у глобального профиля игрока
            if group: # Если группа существует для этой игры
                global_player_profile.last_played_group_id = group.id
            
            global_player_profile.total_games = (global_player_profile.total_games or 0) + 1 # увеличиваем общее количество игр в глобальном профиле


            # Прогрессия уровня для ГЛОБАЛЬНОГО ПРОФИЛЯ
            exp_to_next_level = get_exp_for_next_level(global_player_profile.level)
            while global_player_profile.experience >= exp_to_next_level:
                global_player_profile.level += 1
                global_player_profile.experience -= exp_to_next_level
                try:
                    await bot.send_message(global_player_profile.user_id, f"Поздравляем! Ваш уровень повышен до <b>{global_player_profile.level}</b>! {RESULT_EMOJIS['success']}") 
                except TelegramForbiddenError:
                    logging.warning(f"Бот заблокирован игроком {global_player_profile.full_name}: не удалось отправить уведомление о повышении уровня.")
                except Exception as e:
                    logging.warning(f"Не удалось отправить уведомление о повышении уровня игроку {global_player_profile.full_name}: {e}")
                exp_to_next_level = get_exp_for_next_level(global_player_profile.level)

            session.add(global_player_profile) # Сохраняем изменения глобального профиля

        session.commit()
        logging.info(f"Player stats committed for game {game.id}.")


        logging.info(f"Game {game.id} finished. Status set to 'finished'.")

        for job in scheduler.get_jobs():
            if job.args and job.args[0] == game.id:
                job.remove()
        logging.info(f"All scheduled jobs for game {game.id} removed.")

        final_summary_text = f"<b>{PHASE_EMOJIS['death']} Игра окончена!</b>\n\n"
        final_summary_text += f"{winner_message}\n"

        if winning_faction_players:
            final_summary_text += "Победители:\n"
            for i, p in enumerate(winning_faction_players, 1):
                role_name_ru = ROLE_NAMES_RU.get(p.role, p.role.capitalize())
                role_emoji = ROLE_EMOJIS.get(p.role, "?")
                final_summary_text += f"{i}. {role_emoji} <a href='tg://user?id={p.user_id}'>{p.full_name}</a> - {role_name_ru}\n"
        else:
            final_summary_text += "Никто не считается победителем фракции.\n"

        # Добавляем разделитель после списка победителей
        final_summary_text += "\n" + "—" * 20 + "\n"

        # Детальная сводка по фракциям
        all_players_in_game = session.query(Player).filter_by(game_id=game_id).all()
        mafia_faction_players = [p for p in all_players_in_game if p.role in ['mafia', 'don']]
        civilian_faction_players = [p for p in all_players_in_game if p.role not in ['mafia', 'don', 'maniac']]
        maniac_faction_players = [p for p in all_players_in_game if p.role == 'maniac']

        mafia_list_text = []
        for p in mafia_faction_players:
            role_name_ru = ROLE_NAMES_RU.get(p.role, p.role.capitalize())
            emoji = ROLE_EMOJIS.get(p.role, "?")
            status = f"{PHASE_EMOJIS['death']} (мертв)" if not p.is_alive else ""
            player_link = f"<a href='tg://user?id={p.user_id}'>{p.full_name}</a>"
            mafia_list_text.append(f"{emoji} {player_link} - {role_name_ru} {status}".strip())

        civilian_list_text = []
        for p in civilian_faction_players:
            role_name_ru = ROLE_NAMES_RU.get(p.role, p.role.capitalize())
            emoji = ROLE_EMOJIS.get(p.role, "?")
            status = f"{PHASE_EMOJIS['death']} (мертв)" if not p.is_alive else ""
            player_link = f"<a href='tg://user?id={p.user_id}'>{p.full_name}</a>"
            civilian_list_text.append(f"{emoji} {player_link} - {role_name_ru} {status}".strip())

        maniac_list_text = []
        for p in maniac_faction_players:
            role_name_ru = ROLE_NAMES_RU.get(p.role, p.role.capitalize())
            emoji = ROLE_EMOJIS.get(p.role, "?")
            status = f"{PHASE_EMOJIS['death']} (мертв)" if not p.is_alive else ""
            player_link = f"<a href='tg://user?id={p.user_id}'>{p.full_name}</a>"
            maniac_list_text.append(f"{emoji} {player_link} - {role_name_ru} {status}".strip())

        final_summary_text += f"\n? <b>Итоги по ролям:</b>\n"
        if civilian_list_text:
            final_summary_text += "\n" + FACTION_EMOJIS['town'] + " <b>Мирные жители:</b>\n" + "\n".join(
                civilian_list_text)
            final_summary_text += "\n" + "—" * 20 + "\n"
        if mafia_list_text:
            final_summary_text += "\n" + FACTION_EMOJIS['mafia'] + " <b>Мафия:</b>\n" + "\n".join(mafia_list_text)
            final_summary_text += "\n" + "—" * 20 + "\n"
        if maniac_list_text:
            final_summary_text += "\n" + FACTION_EMOJIS['solo'] + " <b>Одиночки:</b>\n" + "\n".join(maniac_list_text)
            final_summary_text += "\n" + "—" * 20 + "\n"

        final_summary_text += f"\n? <b>Длительность игры:</b> {duration_text}\n"
        final_summary_text += "\n" + "—" * 20 + "\n"

        await bot.send_message(chat_id=game_chat_id, text=final_summary_text, parse_mode=ParseMode.HTML)

        return True
    logging.info(f"Game {game.id} is NOT OVER, continuing.")
    return False
async def send_death_notification_and_farewell_prompt(user_id: int, game_id: int, role: str, full_name: str):
    """Отправляет уведомление о смерти игроку и предлагает написать прощальное сообщение."""
    with Session() as session:
        try:
            player_role_ru = ROLE_NAMES_RU.get(role, role.capitalize())
            role_emoji = ROLE_EMOJIS.get(role, "?")

            await bot.send_message(
                chat_id=user_id,
                text=f"{PHASE_EMOJIS['death']} Увы, <a href='tg://user?id={user_id}'>{full_name}</a> ({role_emoji} {player_role_ru}), вы погибли и покинули игру.\n"
                     f"Вы больше не можете влиять на ход игры, но можете отправить свое последнее прощальное сообщение в общий чат.\n"
                     f"<b>Напишите ваше прощальное сообщение прямо сюда, в наш личный чат.</b>",
                parse_mode=ParseMode.HTML
            )
            state = dp.fsm.get_context(bot=bot, chat_id=user_id, user_id=user_id)
            await state.set_state(GameState.waiting_for_farewell_message)
            await state.update_data(game_id=game_id, player_id=user_id, player_role=role, player_full_name=full_name)
            logging.info(f"Sent death notification and farewell prompt to player {full_name} ({user_id}) for game {game_id}.")

        except TelegramForbiddenError:
            logging.warning(f"Бот заблокирован игроком {full_name} ({user_id}): не удалось отправить уведомление о смерти и запрос прощального сообщения.")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления о смерти и запросе прощального сообщения игроку {user_id}: {e}", exc_info=True)


# --- НОВАЯ ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ensure_player_profile_exists ---
def ensure_player_profile_exists(session, user_id: int, username: str, full_name: str):
    global BOT_ID

    if BOT_ID is None:
        logging.error("CRITICAL: BOT_ID is None when ensure_player_profile_exists is called.")
        return None

    if user_id == BOT_ID:
        logging.warning(f"WARNING: Attempted to get/create global player profile for bot's own ID ({user_id}). Skipping.")
        return None

    try:
        global_player_profile = session.query(Player).filter_by(user_id=user_id, game_id=None).first()
        if not global_player_profile:
            any_player_record = session.query(Player).filter_by(user_id=user_id).first()
            player_gender = any_player_record.gender if any_player_record and any_player_record.gender != 'unspecified' else 'unspecified'

            global_player_profile = Player(
                user_id=user_id,
                username=username,
                full_name=full_name,
                gender=player_gender,
                dollars=0,
                diamonds=0.0,
                # НОВЫЕ ПОЛЯ ДЛЯ КАСТОМИЗАЦИИ
                selected_frame='default',
                selected_title='default',
                unlocked_frames=json.dumps(UNLOCKED_FRAMES_DEFAULT), # Инициализация с дефолтными
                unlocked_titles=json.dumps(UNLOCKED_TITLES_DEFAULT), # Инициализация с дефолтными
                # game_id оставляем None для глобального профиля
            )
            session.add(global_player_profile)
            session.flush()
            logging.info(f"Created global player profile for {full_name} ({user_id}).")
        else:
            global_player_profile.username = username
            global_player_profile.full_name = full_name
            session.add(global_player_profile)
                
        return global_player_profile
    except Exception as e:
        logging.error(f"ERROR: Exception in ensure_player_profile_exists for user {user_id}: {e}", exc_info=True)
        session.rollback()
        return None# Важно вернуть None при ошибке
# ---Хэндлеры команд ---


async def cmd_start(message: Message, command: Command):
    with Session() as session:
        try:
            player_data = ensure_player_profile_exists(session, message.from_user.id, message.from_user.username, message.from_user.full_name)
            if player_data is None:
                logging.error(f"Failed to get/create player profile for user {message.from_user.id}. Possibly bot's own ID.")
                await message.reply(f"Произошла ошибка при загрузке вашего профиля. Попробуйте позже. {FACTION_EMOJIS['missed']}")
                session.rollback()
                return 
            session.commit() # Сохраняем создание/обновление глобального профиля

            if message.chat.type == ChatType.PRIVATE:
                await display_player_profile(message, player_data)
                # Если игрок мертв и застрял в FSM, очищаем его состояние
                # Здесь player_data - это глобальный профиль, у него нет is_alive
                # Нужно искать игрока в текущей игре, если она есть.
                # Изменено: ищем игрока в любой игре, где он участвует, и проверяем его is_alive
                current_game_player = session.query(Player).filter_by(user_id=message.from_user.id, is_alive=False).order_by(Player.id.desc()).first()
                
                if current_game_player: # Если найдена запись игрока, который мертв
                    state = dp.fsm.get_context(bot=bot, chat_id=message.from_user.id, user_id=message.from_user.id)
                    current_state = await state.get_state()
                    if current_state in [GameState.waiting_for_farewell_message, GameState.waiting_for_faction_message,
                                         GameState.waiting_for_donate_group_selection, GameState.waiting_for_donate_currency_selection,
                                         GameState.waiting_for_donate_dollars_amount, GameState.waiting_for_donate_diamonds_amount,
                                         GameState.waiting_for_frame_selection, GameState.waiting_for_title_selection]: # Проверяем все FSM состояния
                        await state.clear()
                        await message.answer(f"Ваше предыдущее состояние в игре было отменено, так как вы мертвы. {PHASE_EMOJIS['death']}")
                        logging.info(f"Cleared FSM state for dead player {player_data.full_name} ({player_data.user_id}).")


                # Отправка ночных кнопок и FSM для чата при /start в ЛС во время ночи
                # Здесь нужно искать игрока в активной игре
                current_game_player = session.query(Player).filter(
                    Player.user_id == message.from_user.id,
                    Player.game_id != None, # Ищем привязанную к игре запись
                    Player.is_alive == True
                ).order_by(Player.id.desc()).first() # Берем последнюю активную игру

                if current_game_player and current_game_player.game and current_game_player.game.status == 'playing' and current_game_player.game.phase == 'night':
                    await _send_night_action_buttons_and_faction_chat_if_needed(message.from_user.id)

                # Информация о текущей ожидающей игре
                player_in_waiting_game = session.query(Player).filter(Player.user_id == message.from_user.id, Player.game_id != None, Player.is_alive == True).first()

                if player_in_waiting_game and player_in_waiting_game.game and player_in_waiting_game.game.status == 'waiting':
                    await bot.send_message(
                        chat_id=message.from_user.id,
                        text=f"Вы уже зарегистрированы в ожидающей игре. Организатор скоро ее начнет. {FACTION_EMOJIS['town']} "
                             f"Текущий чат игры: <code>{player_in_waiting_game.game.chat_id}</code>", parse_mode=ParseMode.HTML
                    )

            else: # Команда /start вызвана в групповом чате
                await message.reply(f"Привет! Я бот для игры в Мафию. {FACTION_EMOJIS['town']}\n"
                                    "Чтобы начать новую игру в этом чате, используйте команду /new_game. \n"
                                    "Для помощи: /help\n"
                                    "<b>Для настройки профиля, пожалуйста, используйте /start в личных сообщениях с ботом.</b>",
                                    parse_mode=ParseMode.HTML)
        except Exception as e:
            logging.error(f"Ошибка при обработке /start для игрока {message.from_user.id}: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()
async def _send_night_action_buttons_and_faction_chat_if_needed(user_id: int):
    """Отправляет или напоминает о ночных кнопках и FSM для чата игроку в ЛС, если это необходимо."""
    with Session() as session:
        try:
            # Ищем игрока в текущей активной игре (не глобальный профиль)
            player = session.query(Player).filter(Player.user_id == user_id,
                                                  Player.game_id != None,
                Player.is_alive == True
            ).order_by(Player.id.desc()).first()

            if not player:
                return

            game = player.game
            if not game or game.status != 'playing' or game.phase != 'night' or player.role not in ['mafia', 'don', 'doctor', 'commissioner', 'maniac']:
                return
            
            # Если игрок уже совершил действие
            if player.night_action_target_id is not None:
                target_player_for_action = session.get(Player, player.night_action_target_id)
                target_name = target_player_for_action.full_name if target_player_for_action else "неизвестный игрок"
                await bot.send_message(
                    chat_id=user_id,
                    text=f"Напоминаю: Вы уже совершили ночное действие. "
                         f"Ваш выбор: <b>{target_name}</b>. "
                         f"Ожидайте утро! {FACTION_EMOJIS['town']}"
                )
                # Для мафии, если они уже выбрали, но могут продолжать чатиться, оставляем FSM
                state = dp.fsm.get_context(bot=bot, chat_id=user_id, user_id=user_id)
                if player.role in ['mafia', 'don']:
                    await state.set_state(GameState.waiting_for_faction_message)
                else:
                    await state.clear() # Для других ролей можно очистить
                return

            players_alive = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()

            target_buttons = []
            keyboard_message = ""

            if player.role == 'mafia' or player.role == 'don':
                keyboard_message = f"Напомню, что наступила ночь. {ROLE_EMOJIS[player.role]} Выберите, кого Мафия убьет этой ночью:"
                possible_targets = [p for p in players_alive if p.role not in ['mafia', 'don'] and p.id != player.id]
                for target in possible_targets:
                    target_buttons.append(
                        InlineKeyboardButton(text=target.full_name, callback_data=f"mafia_kill_{game.id}_{target.id}"))
                
                # Переводим в состояние для чата фракции
                state = dp.fsm.get_context(bot=bot, chat_id=user_id, user_id=user_id)
                await state.set_state(GameState.waiting_for_faction_message)
                await state.update_data(game_id=game.id, player_id=player.id, player_full_name=player.full_name, player_role=player.role)
                await bot.send_message(
                    chat_id=user_id,
                    text=f"Вы находитесь в закрытом чате Мафии. {FACTION_EMOJIS['mafia']} "
                         f"Ваши сообщения, отправленные сюда, будут видны только союзникам."
                )

            elif player.role == 'doctor':
                keyboard_message = f"Напомню, что наступила ночь. {ROLE_EMOJIS['doctor']} Выберите, кого вы вылечите этой ночью:"
                possible_targets = players_alive
                for target in possible_targets:
                    target_buttons.append(
                        InlineKeyboardButton(text=target.full_name, callback_data=f"doctor_heal_{game.id}_{target.id}"))
            elif player.role == 'commissioner':
                keyboard_message = f"Напомню, что наступила ночь. {ROLE_EMOJIS['commissioner']} Выберите, кого вы проверите этой ночью:"
                possible_targets = [p for p in players_alive if p.id != player.id]
                for target in possible_targets:
                    target_buttons.append(InlineKeyboardButton(text=target.full_name, callback_data=f"com_check_{game.id}_{target.id}"))
            elif player.role == 'maniac':
                keyboard_message = f"Напомню, что наступила ночь. {ROLE_EMOJIS['maniac']} Выберите, кого вы убьете этой ночью:"
                possible_targets = [p for p in players_alive if p.id != player.id]
                for target in possible_targets:
                    target_buttons.append(
                        InlineKeyboardButton(text=target.full_name,
                                             callback_data=f"maniac_kill_{game.id}_{target.id}"))

            if target_buttons:
                inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    target_buttons[i:i + 2] for i in range(0, len(target_buttons), 2)
                ])
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=keyboard_message,
                        reply_markup=inline_keyboard
                    )
                    logging.info(f"Reminded player {player.full_name} ({player.role}) about night actions for game {game.id}.")
                except TelegramForbiddenError:
                    logging.warning(f"Бот заблокирован игроком {player.full_name} ({player.user_id}): не удалось отправить напоминание о ночных кнопках.")
                except Exception as e:
                    logging.error(f"Ошибка при повторной отправке ночных кнопок игроку {user_id}: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Ошибка в _send_night_action_buttons_and_faction_chat_if_needed для user {user_id}: {e}", exc_info=True)

 
async def cmd_help(message: Message):
    """
    Обрабатывает команду /help.
    Предоставляет список доступных команд и их описание.
    """
    help_text = (
        "<b>Доступные команды:</b>\n"
        f"/new_game - Начать новую игру в этом чате. (Только в группах) {PHASE_EMOJIS['day']}\n"
        f"/join - Присоединиться к ожидающей игре. {FACTION_EMOJIS['town']}\n"
        f"/leave - Выйти из ожидающей игры. {PHASE_EMOJIS['death']}\n"
        f"/start_game - Начать игру, если набрано достаточно игроков. (Только инициатор или админ) {PHASE_EMOJIS['night']}\n"
        f"/cancel_game - Отменить текущую игру. (Только инициатор или админ) {PHASE_EMOJIS['death']}\n"
        f"/rules - Правила игры в Мафию. {FACTION_EMOJIS['town']}\n"
        f"/players - Посмотреть список живых игроков и их статусы (для игры). {FACTION_EMOJIS['town']}\n"
        f"/profile - Посмотреть свой профиль и статистику. (Только в ЛС) {FACTION_EMOJIS['town']}\n"
        f"/donate - Пожертвовать Доллары/Бриллианты в фонд города. (Только в ЛС) {FACTION_EMOJIS['town']}\n" 
        f"/give_dollars [amount] [@username/ID] - Передать Доллары другому игроку. (В ЛС или группе) {FACTION_EMOJIS['dollars']}\n"
        f"/give_diamonds [amount] [@username/ID] - Передать Бриллианты другому игроку. (В ЛС или группе) {FACTION_EMOJIS['diamonds']}\n"
    )
    await message.reply(help_text, parse_mode=ParseMode.HTML)


 
async def cmd_new_game(message: Message):
    """
    Обрабатывает команду /new_game.
    Создает новую игру в текущем чате, если таковой еще нет.
    Добавляет инлайн-кнопки "Присоединиться" и "Начать игру".
    """
    if message.chat.type == ChatType.PRIVATE:
        await message.reply(f"Эту команду можно использовать только в групповом чате. {PHASE_EMOJIS['day']}")
        return

    with Session() as session:
        try:
            # Всегда гарантируем, что у игрока есть глобальный профиль
            global_player = ensure_player_profile_exists(session, message.from_user.id, message.from_user.username, message.from_user.full_name)
            
            # Ищем ЛЮБУЮ игру с этим chat_id, независимо от статуса
            existing_game_any_status = session.query(Game).filter_by(chat_id=message.chat.id).first()
            
            should_create_new_game = True
            game_to_use = None

            if existing_game_any_status:
                if existing_game_any_status.status in ['waiting', 'playing']:
                    game_to_use = existing_game_any_status
                    should_create_new_game = False
                    logging.info(f"Existing active/waiting game {game_to_use.id} found for chat {message.chat.id}. Updating message.")
                else:
                    logging.info(f"Found non-active game {existing_game_any_status.id} (status: {existing_game_any_status.status}) for chat {message.chat.id}. Deleting it.")
                    
                    for job in scheduler.get_jobs():
                        if job.args and job.args[0] == existing_game_any_status.id:
                            job.remove()
                    logging.info(f"All scheduled jobs for game {existing_game_any_status.id} removed during deletion.")

                    if existing_game_any_status.start_message_id:
                        try:
                            await bot.delete_message(chat_id=existing_game_any_status.chat_id, message_id=existing_game_any_status.start_message_id)
                            logging.info(f"Deleted old start message {existing_game_any_status.start_message_id} for game {existing_game_any_status.id}.")
                        except (TelegramBadRequest, TelegramForbiddenError) as e:
                            logging.warning(f"Could not delete old start message {existing_game_any_status.start_message_id} for game {existing_game_any_status.id}: {e}")
                        except Exception as e:
                            logging.error(f"Unknown error deleting old start message {existing_game_any_status.start_message_id} for game {existing_game_any_status.id}: {e}", exc_info=True)
                    
                    session.delete(existing_game_any_status)
                    session.commit()
                    logging.info(f"Old game {existing_game_any_status.id} (status: {existing_game_any_status.status}) for chat {message.chat.id} successfully deleted.")
            
            if should_create_new_game:
                new_game = Game(chat_id=message.chat.id, status='waiting')
                session.add(new_game)
                session.flush()
                game_to_use = new_game
                logging.info(f"New game {game_to_use.id} created for chat {message.chat.id}.")

                # --- Создание или получение Группы (Города) для чата ---
                group = session.query(Group).filter_by(chat_id=message.chat.id).first()
                if not group:
                    chat_info = await bot.get_chat(message.chat.id)
                    group_name = chat_info.title if chat_info.title else f"Группа {message.chat.id}"
                    group = Group(chat_id=message.chat.id, name=group_name)
                    session.add(group)
                    session.flush() # Получаем ID новой группы до коммита
                    logging.info(f"New Group {group.name} (ID: {group.id}) created for chat {message.chat.id}.")
            
                # Обновляем last_played_group_id для текущего игрока
                # используем глобальный профиль для определения пола
                player_gender = global_player.gender if global_player and global_player.gender != 'unspecified' else random.choice(['male', 'female'])

                player = Player(
                    user_id=message.from_user.id,
                    username=message.from_user.username,
                    full_name=message.from_user.full_name,
                    game_id=game_to_use.id,
                    gender=player_gender,
                    last_played_group_id=group.id # Устанавливаем при создании
                )
                session.add(player)
                session.commit()
                logging.info(f"Player {player.full_name} ({player.user_id}) created and joined new game {game_to_use.id}.")
            else: # Если игра уже существует
                player_in_game_check = session.query(Player).filter_by(user_id=message.from_user.id, game_id=game_to_use.id).first()
                if not player_in_game_check: # Если игрок не в текущей игре
                    # --- Создание или получение Группы (Города) для чата ---
                    group = session.query(Group).filter_by(chat_id=message.chat.id).first()
                    if not group:
                        chat_info = await bot.get_chat(message.chat.id)
                        group_name = chat_info.title if chat_info.title else f"Группа {message.chat.id}"
                        group = Group(chat_id=message.chat.id, name=group_name)
                        session.add(group)
                        session.flush()
                        logging.info(f"New Group {group.name} (ID: {group.id}) created for chat {message.chat.id}.")
                    # --- Конец создания/получения Группы ---

                    player_gender = global_player.gender if global_player and global_player.gender != 'unspecified' else random.choice(['male', 'female'])

                    player = Player(
                        user_id=message.from_user.id,
                        username=message.from_user.username,
                        full_name=message.from_user.full_name,
                        game_id=game_to_use.id,
                        gender=player_gender,
                        last_played_group_id=group.id # Добавляем это
                    )
                    session.add(player)
                    session.commit()
                    logging.info(f"Player {player.full_name} ({player.user_id}) joined existing game {game_to_use.id} via /new_game.")
                # else: # Если игрок уже был в игре, просто обновим его last_played_group_id
                #     # Здесь не нужно обновлять last_played_group_id, так как он уже должен быть установлен
                #     #при первом присоединении к игре.
                #     session.commit() # Просто коммитим, если других изменений не было

            # ОБНОВЛЕНИЕ СООБЩЕНИЯ О РЕГИСТРАЦИИ
            players_in_game = session.query(Player).filter_by(game_id=game_to_use.id).all()
            num_players = len(players_in_game)
            player_names_list = [f"<a href='tg://user?id={p.user_id}'>{p.full_name}</a>" for p in players_in_game]
            player_list_str = "\n".join(player_names_list) if player_names_list else f"Пока нет игроков. {FACTION_EMOJIS['missed']}"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Присоединиться", callback_data=f"join_game_{game_to_use.id}")],
                [InlineKeyboardButton(text=f"Начать игру {PHASE_EMOJIS['night']}",
                                      callback_data=f"start_game_{game_to_use.id}")]
            ])

            organizer = session.query(Player).filter_by(game_id=game_to_use.id).order_by(Player.id).first()
            organizer_link = f"<a href='tg://user?id={organizer.user_id}'>{organizer.full_name}</a>" if organizer else "Неизвестный организатор"

            message_text_to_send = (
                f"Игра в Мафию {'создана' if should_create_new_game else 'ожидается'}! {PHASE_EMOJIS['day']}\n"
                f"Организатор: {organizer_link}\n"
                f"Текущее количество игроков: <b>{num_players}/{MIN_PLAYERS_TO_START}+</b>.\n"
                f"Участники:\n{player_list_str}\n"
                f"Чтобы присоединиться или начать игру, используйте кнопки ниже."
                )

            if game_to_use.start_message_id:
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=game_to_use.start_message_id,
                        text=message_text_to_send,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML
                    )
                    await message.delete()
                    logging.info(f"Updated existing game {game_to_use.id} registration message (ID: {game_to_use.start_message_id}).")
                    return
                except (TelegramBadRequest, TelegramForbiddenError) as e:
                    logging.warning(f"Could not edit existing game registration message {game_to_use.start_message_id} for chat {message.chat.id}: {e}. Sending new message.", exc_info=True)
                    game_to_use.start_message_id = None
                    session.add(game_to_use)
                    session.commit()
                except Exception as e:
                    logging.error(f"Unknown error editing existing game registration message {game_to_use.start_message_id} for chat {message.chat.id}: {e}", exc_info=True)
                    game_to_use.start_message_id = None
                    session.add(game_to_use)
                    session.commit()

            sent_message = await message.reply(
                text=message_text_to_send,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            game_to_use.start_message_id = sent_message.message_id
            session.add(game_to_use)
            session.commit()
            logging.info(f"Sent new game registration message {sent_message.message_id} for game {game_to_use.id}.")

        except Exception as e:
            logging.error(f"Критическая ошибка в cmd_new_game для chat {message.chat.id}: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при создании игры. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()
 
async def cmd_join(message: Message):
    """
    Обрабатывает команду /join.
    Позволяет пользователю присоединиться к ожидающей игре в текущем чате.
    """
    if message.chat.type == ChatType.PRIVATE:
        await message.reply(f"Эту команду можно использовать только в групповом чате. {FACTION_EMOJIS['town']}")
        return

    with Session() as session:
        try:
            # Всегда гарантируем, что у игрока есть глобальный профиль
            global_player = ensure_player_profile_exists(session, message.from_user.id, message.from_user.username, message.from_user.full_name)
            # Если global_player None, значит произошла ошибка или это сам бот. ensure_player_profile_exists уже логирует это.
            if global_player is None:
                await message.reply(f"Произошла ошибка при загрузке вашего профиля. Попробуйте позже. {FACTION_EMOJIS['missed']}")
                session.rollback()
                return
            session.commit() # Сохраняем создание/обновление глобального профиля

            game = session.query(Game).filter_by(chat_id=message.chat.id, status='waiting').first()

            if not game:
                await message.reply(f"В этом чате нет ожидающей игры. Используйте /new_game, чтобы начать. {FACTION_EMOJIS['town']}")
                return

            await _handle_join_game(message.from_user.id, message.from_user.username, message.from_user.full_name, game.id,
                                    message.chat.id, is_callback=False)

        except Exception as e:
            logging.error(f"Ошибка при присоединении к игре через команду: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при присоединении к игре. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()


 
async def callback_join_game(query: CallbackQuery):
    with Session() as session:
        try:
            # Всегда гарантируем, что у игрока есть глобальный профиль
            global_player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
            if global_player is None:
                await bot.send_message(query.message.chat.id,
                                       f"Произошла ошибка при загрузке профиля игрока <a href='tg://user?id={query.from_user.id}'>{query.from_user.full_name}</a>. Попробуйте позже. {FACTION_EMOJIS['missed']}",
                                       parse_mode=ParseMode.HTML)
                await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
                session.rollback()
                return
            session.commit() # Сохраняем создание/обновление глобального профиля

            game_id = int(query.data.split('_')[2])
            await _handle_join_game(query.from_user.id, query.from_user.username, query.from_user.full_name, game_id,
                                    query.message.chat.id, is_callback=True)
            await query.answer()

        except Exception as e:
            logging.error(f"Ошибка при присоединении к игре через кнопку от {query.from_user.id}: {e}", exc_info=True)
            await bot.send_message(query.message.chat.id,
                                   f"Произошла ошибка при присоединении игрока <a href='tg://user?id={query.from_user.id}'>{query.from_user.full_name}</a> к игре. {FACTION_EMOJIS['missed']}",
                                   parse_mode=ParseMode.HTML)
            await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
            session.rollback()


 
async def handle_gif(message: Message):
    if message.chat.type == ChatType.PRIVATE:
        await message.answer(f"File ID вашего GIF: <code>{message.animation.file_id}</code>")

async def _handle_join_game(user_id: int, username: str, full_name: str, game_id: int, chat_id: int,
                             is_callback: bool = False):
    with Session() as session:
        try:
            # Здесь global_player уже должен быть гарантирован через cmd_join или callback_join_game
            global_player_profile = session.query(Player).filter_by(user_id=user_id, game_id=None).first()
            if not global_player_profile:
                # Если функция вызвана напрямую, а глобального профиля нет.
                # ensure_player_profile_exists уже логирует ошибку и возвращает None.
                # Здесь мы уже должны иметь global_player_profile, иначе это ошибка в логике вызова.
                logging.error(f"CRITICAL: Global player profile not found for user {user_id} when attempting to join game {game_id}.")
                alert_text = f"Произошла ошибка: ваш профиль не найден. Пожалуйста, попробуйте команду /start в ЛС и повторите попытку. {FACTION_EMOJIS['missed']}"
                if is_callback:
                    await bot.send_message(chat_id=user_id, text=alert_text)
                else:
                    await bot.send_message(chat_id=chat_id, text=alert_text)
                return


            game = session.get(Game, game_id)
            if not game or game.status != 'waiting':
                
                alert_text = f"Эта игра уже началась или была отменена. {FACTION_EMOJIS['missed']}"
                if is_callback:
                    await bot.send_message(chat_id=user_id, text=alert_text)
                else:
                    await bot.send_message(chat_id=chat_id, text=alert_text)
                return
            # --- Создание или получение Группы (Города) для чата ---
            group = session.query(Group).filter_by(chat_id=chat_id).first()
            if not group:
                chat_info = await bot.get_chat(chat_id)
                group_name = chat_info.title if chat_info.title else f"Группа {chat_id}"
                group = Group(chat_id=chat_id, name=group_name)
                session.add(group)
                session.flush() # Получаем ID новой группы до коммита
                logging.info(f"New Group {group.name} (ID: {group.id}) created for chat {chat_id}.")
            
            # Обновляем last_played_group_id для ГЛОБАЛЬНОГО ПРОФИЛЯ игрока
            # Это важный момент: last_played_group_id теперь должен быть в глобальном профиле, а не в игровом экземпляре,
            # чтобы статистика группы применялась корректно при завершении игры.
            # НО! Для текущей игры, нам все равно нужно last_played_group_id для player_game_instance,
            # чтобы check_win_condition мог получить бонусы.
            # Поэтому, мы будем устанавливать last_played_group_id как в глобальном, так и в игровом экземпляре.
            global_player_profile.last_played_group_id = group.id
            session.add(global_player_profile)


            existing_player_game_instance = session.query(Player).filter_by(user_id=user_id, game_id=game.id).first()
            if existing_player_game_instance:
                alert_text = f"Вы уже в этой игре! {FACTION_EMOJIS['missed']}"
                if is_callback:
                    await bot.send_message(chat_id=user_id, text=alert_text)
                else:
                    await bot.send_message(chat_id=chat_id, text=alert_text)
                return

            # Используем пол из глобального профиля, если он установлен, иначе выбираем случайно
            player_gender = global_player_profile.gender if global_player_profile and global_player_profile.gender != 'unspecified' else random.choice(['male', 'female'])

            # Создаем запись игрока для ЭТОЙ ИГРЫ
            player_game_instance = Player(user_id=user_id, username=username, full_name=full_name, game_id=game.id,
                                          gender=player_gender, last_played_group_id=group.id)
            session.add(player_game_instance)
            session.commit() # Коммитим все изменения, включая глобальный профиль и новый игровой экземпляр
            logging.info(f"Player {full_name} ({user_id}) joined game {game.id}.")

            players_in_game = session.query(Player).filter_by(game_id=game.id).all()
            num_players = len(players_in_game)
            player_names_list = [f"<a href='tg://user?id={p.user_id}'>{p.full_name}</a>" for p in players_in_game]
            player_list_str = "\n".join(player_names_list)

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Присоединиться", callback_data=f"join_game_{game.id})")],
                [InlineKeyboardButton(text=f"Начать игру {PHASE_EMOJIS['night']}",
                                      callback_data=f"start_game_{game.id}")]
            ])

            if game.start_message_id:
                try:
                    organizer = session.query(Player).filter_by(game_id=game.id).order_by(Player.id).first()
                    organizer_link = f"<a href='tg://user?id={organizer.user_id}'>{organizer.full_name}</a>" if organizer else "Неизвестный организатор"
                    await bot.edit_message_text(
                        chat_id=game.chat_id,
                        message_id=game.start_message_id,
                        text=f"Новая игра в Мафию создана! {PHASE_EMOJIS['day']}\n"
                             f"Организатор: {organizer_link}\n"
                             f"Текущее количество игроков: <b>{num_players}/{MIN_PLAYERS_TO_START}+</b>.\n"
                             f"Участники:\n{player_list_str}\n"
                             f"Чтобы присоединиться или начать игру, используйте кнопки ниже.",
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML
                    )
                    logging.info(f"Updated game {game.id} registration message after player {full_name} joined.")
                except (TelegramBadRequest, TelegramForbiddenError) as e:
                    logging.warning(
                        f"Не удалось обновить сообщение о регистрации для игры {game.id} (ID сообщения: {game.start_message_id}): {e}",
                        exc_info=True)
                    await bot.send_message(chat_id=chat_id,
                                           text=f"<a href='tg://user?id={user_id}'>{full_name}</a> присоединился к игре! {FACTION_EMOJIS['town']}\nТекущее количество игроков: <b>{num_players}/{MIN_PLAYERS_TO_START}+</b>",
                                           parse_mode=ParseMode.HTML)
                except Exception as e:
                    logging.error(f"Неизвестная ошибка при редактировании сообщения о регистрации: {e}", exc_info=True)
            else:
                await bot.send_message(chat_id=chat_id, text=f"<a href='tg://user?id={user_id}'>{full_name}</a> присоединился к игре! {FACTION_EMOJIS['town']}\nТекущее количество игроков: <b>{num_players}/{MIN_PLAYERS_TO_START}+</b>",
                                       parse_mode=ParseMode.HTML)

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"Вы успешно присоединились к игре в чате с ID <code>{game.chat_id}</code>! {FACTION_EMOJIS['town']} Ждите начала.",
                    parse_mode=ParseMode.HTML
                )
            except TelegramForbiddenError:
                logging.warning(f"Бот заблокирован игроком {full_name} ({user_id}): не удалось отправить уведомление о присоединении в ЛС.")
            except Exception as e:
                logging.warning(f"Не удалось отправить уведомление о присоединении игроку {full_name} ({user_id}) в ЛС: {e}",
                                exc_info=True)

        except Exception as e:
            logging.error(f"Критическая ошибка в _handle_join_game для user {user_id}, game {game_id}: {e}", exc_info=True)
            await bot.send_message(chat_id=chat_id, text=f"Произошла ошибка при присоединении. {FACTION_EMOJIS['missed']}")
            session.rollback()


 
async def cmd_leave(message: Message):
    """
    Обрабатывает команду /leave.
    Позволяет пользователю выйти из ожидающей игры в текущем чате.
    """
    if message.chat.type == ChatType.PRIVATE:
        await message.reply(f"Эту команду можно использовать только в групповом чате. {PHASE_EMOJIS['death']}")
        return

    with Session() as session:
        try:
            game = session.query(Game).filter_by(chat_id=message.chat.id, status='waiting').first()

            if not game:
                await message.reply(f"В этом чате нет ожидающей игры, из которой можно выйти. {PHASE_EMOJIS['death']}")
                return

            player = session.query(Player).filter_by(
                user_id=message.from_user.id,
                game_id=game.id
            ).first()

            if not player:
                await message.reply(f"Вы не являетесь участником этой игры. {PHASE_EMOJIS['death']}")
            else:
                first_player_in_game = session.query(Player).filter_by(game_id=game.id).order_by(Player.id).first()
                is_organizer = first_player_in_game and first_player_in_game.user_id == message.from_user.id

                session.delete(player)
                session.commit()
                logging.info(f"Player {message.from_user.full_name} ({message.from_user.id}) left game {game.id}.")

                num_players = session.query(Player).filter_by(game_id=game.id).count()
                
                await bot.send_message(
                    chat_id=message.chat.id,
                    text=f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a> покинул игру. {PHASE_EMOJIS['death']}\n"
                         f"Текущее количество игроков: <b>{num_players}/{MIN_PLAYERS_TO_START}+</b>",
                    parse_mode=ParseMode.HTML
                )
                
                if num_players == 0:
                    session.delete(game)
                    session.commit()
                    await bot.send_message(chat_id=message.chat.id,
                                           text=f"Все игроки покинули игру. Игра отменена. {PHASE_EMOJIS['death']}")
                    try:
                        if game.start_message_id:
                            await bot.delete_message(chat_id=game.chat_id, message_id=game.start_message_id)
                            logging.info(f"Deleted game {game.id} start message as no players left.")
                    except (TelegramBadRequest, TelegramForbiddenError) as e:
                        logging.warning(f"Не удалось удалить стартовое сообщение игры {game.id}: {e}")
                    except Exception as e:
                        logging.error(f"Неизвестная ошибка при удалении стартового сообщения: {e}")
                elif is_organizer:
                    next_organizer = session.query(Player).filter_by(game_id=game.id).order_by(Player.id).first()
                    if next_organizer:
                        await bot.send_message(chat_id=message.chat.id,
                                               text=f"Организатор покинул игру. Новым организатором становится <a href='tg://user?id={next_organizer.user_id}'>{next_organizer.full_name}</a>. {FACTION_EMOJIS['town']}",
                                               parse_mode=ParseMode.HTML)


        except Exception as e:
            logging.error(f"Ошибка при выходе из игры: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при выходе из игры. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()


 
async def cmd_start_game(message: Message):
    """
    Обрабатывает команду /start_game.
    Начинает игру, если набрано достаточно игроков и игра находится в статусе 'waiting'.
    """
    if message.chat.type == ChatType.PRIVATE:
        await message.reply(f"Эту команду можно использовать только в групповом чате. {PHASE_EMOJIS['day']}")
        return

    await _handle_start_game(message.from_user.id, message.chat.id, is_callback=False)


 
async def callback_start_game(query: CallbackQuery):
    await _handle_start_game(query.from_user.id, query.message.chat.id, is_callback=True)
    await query.answer()


async def _handle_start_game(user_id: int, chat_id: int, is_callback: bool = False):
    with Session() as session:
        try:
            game = session.query(Game).filter_by(chat_id=chat_id, status='waiting').first()
            if not game:
                await bot.send_message(chat_id=chat_id, text=f"В этом чате нет ожидающей игры. Используйте /new_game, чтобы начать. {PHASE_EMOJIS['day']}")
                return

            players = session.query(Player).filter_by(game_id=game.id).all()
            num_players = len(players)

            if num_players < MIN_PLAYERS_TO_START:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Недостаточно игроков для начала игры! {PHASE_EMOJIS['day']}\n"
                         f"Минимальное количество игроков: {MIN_PLAYERS_TO_START}\n"
                         f"Текущее количество: {num_players}"
                )
                return

            first_player = session.query(Player).filter_by(game_id=game.id).order_by(Player.id).first()
            is_chat_admin = False
            try:
                chat_member = await bot.get_chat_member(chat_id, user_id)
                if chat_member.status in ['administrator', 'creator']:
                    is_chat_admin = True
            except Exception as e:
                logging.warning(f"Не удалось проверить права администратора для пользователя {user_id} в чате {chat_id}: {e}")

            if not first_player or (first_player.user_id != user_id and not is_chat_admin):
                await bot.send_message(chat_id=chat_id,
                                       text=f"Только организатор игры (<a href='tg://user?id={first_player.user_id}'>{first_player.full_name}</a>) или администратор чата может начать игру. {PHASE_EMOJIS['day']}", parse_mode=ParseMode.HTML)
                return

            role_distribution = get_roles_distribution(num_players)
            if not role_distribution:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Не удалось распределить роли для {num_players} игроков. "
                         f"Проверьте конфигурацию ролей. {FACTION_EMOJIS['missed']}"
                )
                return

            roles_to_assign = []
            for role_name, count in role_distribution.items():
                roles_to_assign.extend([role_name] * count)
            random.shuffle(roles_to_assign)

            # ЭТАП 1: Присваиваем роли и явно устанавливаем is_alive, затем коммитим
            for i, player in enumerate(players):
                player.role = roles_to_assign[i]
                player.is_alive = True
                # total_games, total_wins, experience, dollars, etc. теперь обновляются в ГЛОБАЛЬНОМ ПРОФИЛЕ после игры.
                # Здесь только устанавливаем role и is_alive для ИГРОВОЙ записи.
                session.add(player)
            
            session.commit()
            logging.info(f"Roles assigned and all players committed as alive for game {game.id}.")

            # ЭТАП 2: Отправляем информацию о ролях и СОЮЗНИКАХ
            for player in players:
                try:
                    role_name_ru = ROLE_NAMES_RU.get(player.role, player.role.capitalize())
                    role_emoji = ROLE_EMOJIS.get(player.role, "?")
                    role_description = ROLES_CONFIG.get(player.role, {}).get('description', 'Роль без описания.')
                    
                    await bot.send_message(
                        chat_id=player.user_id,
                        text=f"Добро пожаловать в игру в Мафию!\n"
                             f"Ваша роль: {role_emoji} <b>{role_name_ru}</b>.\n"
                             f"{role_description}",
                        parse_mode=ParseMode.HTML
                    )
                    
                    if player.role in ['mafia', 'don']:
                        mafia_faction_members = session.query(Player).filter(
                            Player.game_id == game.id,
                            Player.is_alive == True,
                            Player.role.in_(['mafia', 'don'])
                        ).all()

                        if mafia_faction_members:
                            faction_list_text = "<b>Твоя фракция Мафии:</b>\n" 
                            for member in mafia_faction_members:
                                member_role_ru = ROLE_NAMES_RU.get(member.role, member.role.capitalize())
                                member_role_emoji = ROLE_EMOJIS.get(member.role, "?")
                                if member.user_id == player.user_id:
                                    faction_list_text += f"- {member_role_emoji} <a href='tg://user?id={member.user_id}'>{member.full_name}</a> - {member_role_ru} (Ты)\n"
                                else:
                                    faction_list_text += f"- {member_role_emoji} <a href='tg://user?id={member.user_id}'>{member.full_name}</a> - {member_role_ru}\n"
                            await bot.send_message(chat_id=player.user_id, text=faction_list_text, parse_mode=ParseMode.HTML)
                        else:
                            await bot.send_message(chat_id=player.user_id, text="Ты единственный в своей фракции Мафии в этой игре.")

                except TelegramForbiddenError:
                    logging.error(f"Бот заблокирован игроком {player.full_name} ({player.user_id}): не удалось отправить роль/союзников. Игра не может быть начата.", exc_info=True)
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"Не могу отправить роль игроку <a href='tg://user?id={player.user_id}'>{player.full_name}</a> в личные сообщения. "
                             f"Пожалуйста, попросите его написать мне /start в личном чате и попробуйте снова. "
                             f"Это критично для игры! {PHASE_EMOJIS['day']} Игра отменена.",
                        parse_mode=ParseMode.HTML
                    )
                    session.rollback()
                    game.status = 'cancelled'
                    session.add(game)
                    session.commit()
                    return
                except Exception as e:
                    logging.error(f"Не удалось отправить роль/союзников игроку {player.full_name} ({player.user_id}): {e}",
                                  exc_info=True)
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"Не могу отправить роль игроку <a href='tg://user?id={player.user_id}'>{player.full_name}</a> в личные сообщения. "
                             f"Пожалуйста, попросите его написать мне /start в личном чате и попробуйте снова. "
                             f"Это критично для игры! {PHASE_EMOJIS['day']} Игра отменена.",
                        parse_mode=ParseMode.HTML
                    )
                    session.rollback()
                    game.status = 'cancelled'
                    session.add(game)
                    session.commit()
                    return

            # ЭТАП 3: Устанавливаем статус игры на 'playing' и коммитим
            game.status = 'playing'
            game.start_actual_time = datetime.datetime.now()
            game.current_day = 1
            game.phase = 'night'
            game.phase_end_time = datetime.datetime.now() + datetime.timedelta(seconds=PHASE_DURATIONS['night'])
            game.voted_for_player_id = None
            game.lynch_vote_likes = 0
            game.lynch_vote_dislikes = 0
            game.lynch_message_id = None
            game.lynch_voters = ""
            session.add(game)
            session.commit()
            logging.info(f"Game {game.id} started. Transitioned to 'night' phase.")

            try:
                if game.start_message_id:
                    await bot.delete_message(chat_id=chat_id, message_id=game.start_message_id)
                    game.start_message_id = None
                    session.add(game)
                    session.commit()
                    logging.info(f"Deleted game {game.id} start message.")
            except (TelegramBadRequest, TelegramForbiddenError) as e:
                logging.warning(f"Не удалось удалить стартовое сообщение игры {game.id}: {e}")
            except Exception as e:
                logging.error(f"Неизвестная ошибка при удалении стартового сообщения: {e}")
            
            player_names_clickable = [f"<a href='tg://user?id={p.user_id}'>{p.full_name}</a>" for p in players]
            await bot.send_message(
                chat_id=chat_id,
                text=f"Игра начинается! {PHASE_EMOJIS['night']}\n"
                     f"Участники: {', '.join(player_names_clickable)}\n\n"
                f"<b>Наступила ночь!</b> {PHASE_EMOJIS['night']} Проверьте свои личные сообщения от бота для совершения ночных действий.",
                parse_mode=ParseMode.HTML
            )

            await start_night_phase(game.id)
            scheduler.add_job(end_night_phase_processing, 'date', run_date=game.phase_end_time, args=[game.id],
                              id=f"end_night_processing_game_{game.id}")
            logging.info(f"Scheduled end_night_phase_processing for game {game.id} at {game.phase_end_time}.")

        except Exception as e:
            logging.error(f"Ошибка при старте игры: {e}", exc_info=True)
            await bot.send_message(chat_id=chat_id, text=f"Произошла ошибка при начале игры. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()


 
async def cmd_cancel_game(message: Message):
    """
    Обрабатывает команду /cancel_game.
    Отменяет текущую игру в чате.
    """
    if message.chat.type == ChatType.PRIVATE:
        await message.reply(f"Эту команду можно использовать только в групповом чате. {PHASE_EMOJIS['death']}")
        return

    with Session() as session:
        try:
            game = session.query(Game).filter(
                Game.chat_id == message.chat.id,
                Game.status.in_(['waiting', 'playing'])
            ).first()

            if not game:
                await message.reply(f"В этом чате нет активной игры для отмены. {PHASE_EMOJIS['death']}")
                return

            can_cancel = False
            first_player = session.query(Player).filter_by(game_id=game.id).order_by(Player.id).first()
            
            is_chat_admin = False
            try:
                chat_member = await bot.get_chat_member(message.chat.id, message.from_user.id)
                if chat_member.status in ['administrator', 'creator']:
                    is_chat_admin = True
            except Exception as e:
                logging.warning(f"Не удалось проверить права администратора для пользователя {message.from_user.id} в чате {message.chat.id}: {e}")

            if game.status == 'waiting':
                if first_player and first_player.user_id == message.from_user.id:
                    can_cancel = True
                elif is_chat_admin:
                    can_cancel = True
            elif game.status == 'playing':
                player_in_game = session.query(Player).filter_by(user_id=message.from_user.id, game_id=game.id).first()
                if player_in_game:
                    can_cancel = True
                elif is_chat_admin:
                    can_cancel = True

            if not can_cancel:
                await message.reply(
                    f"Только организатор игры ({first_player.full_name if first_player else 'неизвестно'}) или администратор чата может отменить игру. {PHASE_EMOJIS['day']}",
                    parse_mode=ParseMode.HTML
                )
                return

            for job in scheduler.get_jobs():
                if job.args and job.args[0] == game.id:
                    job.remove()
            logging.info(f"All scheduled jobs for game {game.id} removed due to cancellation.")

            game.status = 'cancelled'
            session.add(game)
            session.commit()
            logging.info(f"Game {game.id} cancelled by player {message.from_user.full_name}.")

            players_in_cancelled_game = session.query(Player).filter_by(game_id=game.id).all()
            for player in players_in_cancelled_game:
                try:
                    await bot.send_message(player.user_id,
                                           f"Игра, в которой вы участвовали, была отменена. {PHASE_EMOJIS['death']}")
                except TelegramForbiddenError:
                    logging.warning(f"Бот заблокирован игроком {player.full_name}: не удалось отправить уведомление об отмене.")
                except Exception as e:
                    logging.warning(
                        f"Не удалось отправить уведомление об отмене игры игроку {player.full_name}: {e}", exc_info=True)

            await message.reply(f"Игра отменена. {PHASE_EMOJIS['death']}")
            try:
                if game.start_message_id:
                    await bot.delete_message(chat_id=game.chat_id, message_id=game.start_message_id)
                    game.start_message_id = None
                    session.add(game)
                    session.commit()
                    logging.info(f"Deleted game {game.id} start message after cancellation.")
            except (TelegramBadRequest, TelegramForbiddenError) as e:
                logging.warning(f"Не удалось удалить стартовое сообщение игры {game.id}: {e}")
            except Exception as e:
                logging.error(f"Неизвестная ошибка при удалении стартового сообщения: {e}")


        except Exception as e:
            logging.error(f"Ошибка при отмене игры: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при отмене игры. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()


 
async def cmd_players(message: Message):
    """
    Показывает список живых и мертвых игроков в текущей игре.
    """
    if message.chat.type == ChatType.PRIVATE:
        await message.reply(f"Эту команду можно использовать только в групповом чате. {FACTION_EMOJIS['town']}")
        return

    with Session() as session:
        try:
            game = session.query(Game).filter_by(chat_id=message.chat.id, status='playing').first()

            if not game:
                await message.reply(f"В этом чате нет активной игры. {PHASE_EMOJIS['death']}")
                return

            players_alive = session.query(Player).filter_by(game_id=game.id, is_alive=True).all()
            players_dead = session.query(Player).filter_by(game_id=game.id, is_alive=False).all()

            if not players_alive and not players_dead:
                await message.reply(f"В этой игре пока нет игроков. {FACTION_EMOJIS['missed']}")
                return

            response_text = f"<b>? Список игроков:</b>\n\n"
            if players_alive:
                response_text += f"<b>{FACTION_EMOJIS['town']} Живые:</b>\n"
                for p in players_alive:
                    gender_emoji = GENDER_EMOJIS.get(p.gender, "?")
                    response_text += f"- {gender_emoji} <a href='tg://user?id={p.user_id}'>{p.full_name}</a>\n"

            if players_dead:
                response_text += f"\n<b>{PHASE_EMOJIS['death']} Мертвые:</b>\n"
                for p in players_dead:
                    dead_player_role_ru = ROLE_NAMES_RU.get(p.role, p.role.capitalize())
                    role_emoji = ROLE_EMOJIS.get(p.role, "?")
                    response_text += f"- {PHASE_EMOJIS['death']} <a href='tg://user?id={p.user_id}'>{p.full_name}</a> (<i>{role_emoji} {dead_player_role_ru}</i>)\n"
            await message.reply(response_text, parse_mode=ParseMode.HTML)
            logging.info(f"Player {message.from_user.full_name} requested player list for game {game.id}.")

        except Exception as e:
            logging.error(f"Ошибка при получении списка игроков: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при получении списка игроков. Попробуйте позже. {FACTION_EMOJIS['missed']}")


 
async def cmd_profile(message: Message):
    if message.chat.type != ChatType.PRIVATE:
        await message.reply(f"Для просмотра профиля используйте эту команду в личной переписке с ботом. {FACTION_EMOJIS['town']}")
        return

    with Session() as session:
        try:
            player_data = ensure_player_profile_exists(session, message.from_user.id, message.from_user.username, message.from_user.full_name)
            if player_data is None:
                logging.error(f"Failed to get/create player profile for user {message.from_user.id}. Possibly bot's own ID.")
                await message.reply(f"Произошла ошибка при загрузке вашего профиля. Попробуйте позже. {FACTION_EMOJIS['missed']}")
                session.rollback()
                return 
            session.commit() # Сохраняем создание/обновление глобального профиля

            await display_player_profile(message, player_data) 
            logging.info(f"Player {message.from_user.full_name} requested their profile.")

        except Exception as e:
            logging.error(f"Ошибка при получении профиля игрока {message.from_user.id}: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при загрузке профиля. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()
async def display_player_profile(message: Message, player_data: Player):
    """
    Формирует и отправляет сообщение с профилем игрока, используя новые разделители и полную статистику.
    player_data здесь - это ГЛОБАЛЬНЫЙ ПРОФИЛЬ (game_id=None).
    """
    # Проверка, является ли этот игрок владельцем бота
    is_owner = (player_data.user_id == BOT_OWNER_ID)

    # Исправленный расчет Win Rate
    win_rate = 0.0
    if player_data.total_games and player_data.total_games > 0:
        win_rate = (player_data.total_wins / player_data.total_games) * 100
    win_rate = min(win_rate, 100.0) # Ограничиваем до 100%

    # Исправленный расчет K/D Ratio
    total_deaths = player_data.total_deaths or 0
    total_kills = player_data.total_kills or 0
    
    kd_ratio_text = "0.00"
    if total_deaths == 0:
        if total_kills > 0:
            kd_ratio_text = "?" # Символ бесконечности
        else:
            kd_ratio_text = "0.00"
    else:
        kd_ratio_text = f"{(total_kills / total_deaths):.2f}"
    
    # Опыт до следующего уровня
    exp_to_next_level_val = get_exp_for_next_level(player_data.level)
    remaining_exp = exp_to_next_level_val - player_data.experience

    gender_emoji = GENDER_EMOJIS.get(player_data.gender, "?")
    gender_name = ROLE_NAMES_RU.get(player_data.gender, "Не указан")

    # Получаем выбранную рамку (или дефолтную)
    # Используем CUSTOM_FRAMES для получения деталей рамки
    selected_frame_key = player_data.selected_frame if player_data.selected_frame in CUSTOM_FRAMES else "default"
    frame = CUSTOM_FRAMES[selected_frame_key]
    divider_top = frame["top"]
    divider_middle = frame["middle"]
    divider_bottom = frame["bottom"]

    group_info_text = ""
    with Session() as session:
        if player_data.last_played_group_id:
            group_obj = session.query(Group).filter_by(id=player_data.last_played_group_id).first()
            if group_obj:
                group_info_text = f"<b>{format_group_info(group_obj)}</b>"
    
    # Титул игрока
    player_title_text = ""
    selected_title_key = player_data.selected_title if player_data.selected_title in CUSTOM_TITLES else "default"
    title_info = CUSTOM_TITLES[selected_title_key]
    player_title_text = f"{title_info['emoji']} [{title_info['name_ru']}] "


    dollars_display = "?" if is_owner else str(player_data.dollars)
    diamonds_display = "?" if is_owner else f"{player_data.diamonds:.2f}"
    profile_text = (
        f"<code>{divider_top}</code>\n"
        f"?? Профиль игрока: {player_title_text}<b>{player_data.full_name}</b>\n"
        f"<code>{divider_middle}</code>\n"
        f"?? ID: <code>{player_data.user_id}</code>\n"
        f"? Ник: @{player_data.username if player_data.username else 'нет'}\n"
        f"?? Пол: {gender_emoji} {gender_name}\n"
        f"<code>{divider_middle}</code>\n"
        f"{group_info_text}" # Информация о группе
        f"{FACTION_EMOJIS['dollars']} Долларов: {dollars_display}\n" 
        f"{FACTION_EMOJIS['diamonds']} Бриллиантов: {diamonds_display}\n" 
        f"<code>{divider_middle}</code>\n"
        f"?? Уровень: {player_data.level}\n"
        f"? Опыт: {player_data.experience:.0f} (до след. ур: {max(0, remaining_exp):.0f})\n"
        f"<code>{divider_middle}</code>\n"
        f"?? Количество побед: {player_data.total_wins}\n"
        f"?? Общее количество игр: {player_data.total_games}\n"
        f"?? Win Rate (WR): {win_rate:.2f}%\n"
        f"?? Kill/Death (KD): {kd_ratio_text}\n"
        f"<code>{divider_bottom}</code>"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Пол {GENDER_EMOJIS['unspecified']}", callback_data="set_gender_prompt")],
        [InlineKeyboardButton(text=f"?? Пожертвовать", callback_data="donate_prompt")],
        [InlineKeyboardButton(text=f"??? Рамки", callback_data="select_frame_prompt")], # НОВАЯ КНОПКА
        [InlineKeyboardButton(text=f"??? Титулы", callback_data="select_title_prompt")] # НОВАЯ КНОПКА
    ])

    await message.reply(profile_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

# --- НОВЫЙ ХЭНДЛЕР ДЛЯ ВЫБОРА ГРУППЫ ДЛЯ ПОЖЕРТВОВАНИЯ ---
  
async def callback_select_donate_group(query: CallbackQuery, state: FSMContext):
    with Session() as session:
        try:
            group_id = int(query.data.split('_')[3])
            selected_group = session.get(Group, group_id)
            fsm_data = await state.get_data()
            user_id_from_fsm = fsm_data.get('user_id') 
            
            logging.debug(f"DEBUG: In callback_select_donate_group. user_id_from_fsm: {user_id_from_fsm}, query.from_user.id: {query.from_user.id}")

            player = session.query(Player).filter_by(user_id=user_id_from_fsm, game_id=None).first() 
                
            if player:
                logging.debug(f"DEBUG: Player (global profile) found. ID: {player.id}, User_ID: {player.user_id}, Full Name: {player.full_name}, Game_ID: {player.game_id}")
            else:
                logging.debug(f"DEBUG: Player (global profile) NOT FOUND for user_id {user_id_from_fsm}.")
                # Если player не найден, отправляем alert и выходим
                await query.answer("Ошибка: Профиль игрока не найден. Попробуйте снова через /donate.", show_alert=True)
                await state.clear()
                return

            if not selected_group or selected_group.id != group_id: # Добавил проверку, что найденная группа соответствует ID
                logging.warning(f"WARNING: Selected group not found or ID mismatch. selected_group: {bool(selected_group)}, group_id: {group_id}")
                await query.answer("Ошибка выбора группы. Группа не найдена. Попробуйте снова через /donate.", show_alert=True)
                await state.clear()
                return
            
            # Формируем информацию о текущих средствах
            player_dollars_display = "??" if player.user_id == BOT_OWNER_ID else str(player.dollars)
            player_diamonds_display = "??" if player.user_id == BOT_OWNER_ID else f"{player.diamonds:.2f}"

            group_details = format_group_info(selected_group)
            logging.debug(f"DEBUG: Formatted group info: {group_details}") # Логируем результат format_group_info

            keyboard_currency = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"Пожертвовать Доллары {FACTION_EMOJIS['dollars']}", callback_data=f"donate_currency_dollars_{selected_group.id}")],
                [InlineKeyboardButton(text=f"Пожертвовать Бриллианты {FACTION_EMOJIS['diamonds']}", callback_data=f"donate_currency_diamonds_{selected_group.id}")]
            ])

            try:
                await query.message.edit_text(
                    f"{group_details}\n"
                    f"Выберите валюту для пожертвования в Фонд Города.\n"
                    f"Ваши текущие средства: {player_dollars_display} Долларов, {player_diamonds_display} Бриллиантов.",
                    reply_markup=keyboard_currency,
                    parse_mode=ParseMode.HTML
                )
                await state.set_state(GameState.waiting_for_donate_currency_selection)
                await state.update_data(user_id=player.user_id, selected_group_id=selected_group.id)

                await query.answer(f"Выбрана группа: {selected_group.name}.", show_alert=False)
                logging.info(f"Player {player.full_name} ({player.user_id}) selected group {selected_group.name} for donate, now choosing currency.")

            except TelegramBadRequest as e:
                logging.error(f"TelegramBadRequest при edit_text в callback_select_donate_group: {e}", exc_info=True)
                await query.answer("Ошибка при обновлении сообщения. Возможно, оно слишком старое или было изменено.", show_alert=True)
                await state.clear()
            except Exception as e:
                logging.error(f"Неизвестная ошибка при редактировании сообщения или установке FSM в callback_select_donate_group: {e}", exc_info=True)
                await query.answer("Произошла ошибка при обновлении сообщения.", show_alert=True)
                await state.clear()

        except Exception as e: 
                logging.error(f"Критическая ошибка в callback_select_donate_group для {query.from_user.id}: {e}", exc_info=True)
                await query.answer("Произошла критическая ошибка при выборе группы.", show_alert=True)
                await state.clear()
                session.rollback()

  
async def callback_donate_currency_selection(query: CallbackQuery, state: FSMContext):
    with Session() as session:
        try:
            parts = query.data.split('_')
            currency_type = parts[2] # 'dollars' или 'diamonds'
            group_id = int(parts[3])
            fsm_data = await state.get_data()
            user_id_from_fsm = fsm_data.get('user_id')
            
            # --- НОВЫЕ ЛОГИ ---
            logging.debug(f"DEBUG_CURRENCY_SEL: query.from_user.id: {query.from_user.id}")
            logging.debug(f"DEBUG_CURRENCY_SEL: user_id_from_fsm (from state): {user_id_from_fsm}")
            logging.debug(f"DEBUG_CURRENCY_SEL: selected_group_id (from state, should be {group_id}): {fsm_data.get('selected_group_id')}")
            # --- КОНЕЦ НОВЫХ ЛОГОВ ---

            player = session.query(Player).filter_by(user_id=user_id_from_fsm, game_id=None).first()
            selected_group = session.get(Group, group_id)

            # Перепроверяем на соответствие ID, это наш "ключ"
            if player and player.user_id != user_id_from_fsm:
                 logging.error(f"CRITICAL_ERROR: Player object user_id ({player.user_id}) does not match user_id_from_fsm ({user_id_from_fsm}) after query. This should not happen!")
                 # Здесь можно даже сбросить FSM или запросить перелогин, т.к. это очень странная ошибка

            if not player or player.user_id != query.from_user.id or not selected_group:
                logging.warning(f"WARNING: Player or group not found/mismatch in callback_donate_currency_selection. "
                                f"Player: {bool(player)}, Group: {bool(selected_group)}, "
                                f"User ID match: {player.user_id == query.from_user.id if player else 'N/A'}. "
                                f"Expected user_id: {query.from_user.id}, Actual player.user_id: {player.user_id if player else 'N/A'}")
                await query.answer("Ошибка: Профиль игрока или группа не найдены. Попробуйте снова через /donate.", show_alert=True)
                await state.clear()
                return

            await state.update_data(selected_currency_type=currency_type)

            player_dollars_display = "??" if player.user_id == BOT_OWNER_ID else str(player.dollars)
            player_diamonds_display = "??" if player.user_id == BOT_OWNER_ID else f"{player.diamonds:.2f}"

            group_details_for_display = format_group_info(selected_group)
            logging.debug(f"DEBUG_CURRENCY_SEL: Formatted group info for currency selection: {group_details_for_display}")


            message_text = ""
            if currency_type == 'dollars':
                message_text = (
                    f"{group_details_for_display}\n"
                    f"Вы выбрали пожертвовать Доллары {FACTION_EMOJIS['dollars']}.\n"
                    f"Ваши текущие Доллары: {player_dollars_display}{FACTION_EMOJIS['dollars']}.\n"
                    f"Введите сумму Долларов для пожертвования:"
                )
                await state.set_state(GameState.waiting_for_donate_dollars_amount)
            elif currency_type == 'diamonds':
                message_text = (
                    f"{group_details_for_display}\n"
                    f"Вы выбрали пожертвовать Бриллианты {FACTION_EMOJIS['diamonds']}.\n"
                    f"Ваши текущие Бриллианты: {player_diamonds_display}{FACTION_EMOJIS['diamonds']}.\n"
                    f"Введите сумму Бриллиантов для пожертвования (можно использовать дробные числа, например, 0.5):"
                )
                await state.set_state(GameState.waiting_for_donate_diamonds_amount)
            else:
                await query.answer("Неизвестная валюта. Попробуйте снова.", show_alert=True)
                await state.clear()
                return

            try:
                await query.message.edit_text(
                    text=message_text,
                    reply_markup=None,
                    parse_mode=ParseMode.HTML
                )
                await query.answer(f"Выбраны {'Доллары' if currency_type == 'dollars' else 'Бриллианты'}.", show_alert=False)
                logging.info(f"Player {player.full_name} ({player.user_id}) chose to donate {currency_type} to group {selected_group.name}.")

            except TelegramBadRequest as e:
                logging.error(f"TelegramBadRequest при edit_text в callback_donate_currency_selection: {e}", exc_info=True)
                await query.answer("Ошибка при обновлении сообщения. Возможно, оно слишком старое или было изменено.", show_alert=True)
                await state.clear()
            except Exception as e:
                logging.error(f"Неизвестная ошибка при редактировании сообщения или установке FSM в callback_donate_currency_selection: {e}", exc_info=True)
                await query.answer("Произошла ошибка при обновлении сообщения.", show_alert=True)
                await state.clear()

        except Exception as e:
            logging.error(f"Критическая ошибка в callback_donate_currency_selection для {query.from_user.id}: {e}", exc_info=True)
            await query.answer("Произошла критическая ошибка при выборе валюты.", show_alert=True)
            await state.clear()
            session.rollback()
 
async def callback_set_gender_prompt(query: CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Мужской {GENDER_EMOJIS['male']}", callback_data="set_gender_male")],
        [InlineKeyboardButton(text=f"Женский {GENDER_EMOJIS['female']}", callback_data="set_gender_female")]
    ])
    await query.message.edit_text("Выберите ваш пол:", reply_markup=keyboard, parse_mode=ParseMode.HTML)
    await query.answer()


 
async def callback_set_gender(query: CallbackQuery):
    with Session() as session:
        try:
            # Ищем глобальный профиль
            player = session.query(Player).filter_by(user_id=query.from_user.id, game_id=None).first()
            if not player:
                await query.answer("Ваш профиль не найден.", show_alert=True)
                # Это должно быть невозможно, если ensure_player_profile_exists работает
                return

            gender_chosen = query.data.split('_')[2]
            player.gender = gender_chosen
            session.add(player)
            session.commit()
            logging.info(f"Player {query.from_user.full_name} ({query.from_user.id}) set gender to {gender_chosen}.")

            gender_emoji = GENDER_EMOJIS.get(gender_chosen, "?")
            gender_name_ru = ROLE_NAMES_RU.get(gender_chosen, "Не указан")
            await query.message.edit_text(
                f"Ваш пол установлен как {gender_emoji} <b>{gender_name_ru}</b>. Чтобы обновить профиль, используйте команду /profile.",
                reply_markup=None, parse_mode=ParseMode.HTML)
            await query.answer("Пол успешно установлен.", show_alert=False)
        except Exception as e:
            logging.error(f"Ошибка при установке пола для игрока {query.from_user.id}: {e}", exc_info=True)
            await query.answer("Произошла ошибка при установке пола.", show_alert=True)
            session.rollback()


# Удалите декоратор @dp.message(Command("donate")) из cmd_donate
# и переименуйте ее в функцию, которая не является обработчиком.
# Назовем ее _process_donate_command_logic
async def _process_donate_command_logic(user_id: int, username: str, full_name: str, chat_id: int, state: FSMContext, reply_sender_func):
    """
    Обрабатывает основную логику для команды /donate.
    reply_sender_func - это асинхронная функция, которая принимает текст и отправляет ответ.
    """
    with Session() as session:
        player = ensure_player_profile_exists(session, user_id, username, full_name)
        if player is None:
            logging.error(f"Не удалось получить/создать профиль игрока для пользователя {user_id}. Возможно, это ID самого бота.")
            await reply_sender_func(f"Произошла ошибка при загрузке вашего профиля. Попробуйте позже. {FACTION_EMOJIS['missed']}")
            session.rollback()
            return
        session.commit()

        # Получаем все группы из базы данных
        all_groups = session.query(Group).all()

        # Формируем информацию о текущих средствах отправителя
        player_dollars_display = "??" if player.user_id == BOT_OWNER_ID else str(player.dollars)
        player_diamonds_display = "??" if player.user_id == BOT_OWNER_ID else f"{player.diamonds:.2f}"

        if not all_groups:
            await reply_sender_func(f"К сожалению, пока нет ни одной зарегистрированной группы, в которую можно пожертвовать. {FACTION_EMOJIS['missed']}\n"
                                f"Чтобы добавить группу, пригласите меня в чат и создайте новую игру с помощью /new_game.", parse_mode=ParseMode.HTML)
            await state.clear()
            return

        # Всегда показываем кнопки выбора групп, даже если группа одна
        keyboard_buttons = []
        for group_obj in all_groups:
            keyboard_buttons.append(InlineKeyboardButton(text=f"{group_obj.name} (Ур. {group_obj.level})", callback_data=f"select_donate_group_{group_obj.id}"))

        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            keyboard_buttons[i:i + 1] for i in range(0, len(keyboard_buttons), 1)])
        await reply_sender_func(
            f"Выберите, в Фонд какого Города вы хотите пожертвовать:\n"
            f"Ваши текущие средства: {player_dollars_display} Долларов, {player_diamonds_display} Бриллиантов.",
            reply_markup=inline_keyboard,
            parse_mode=ParseMode.HTML
        )
        await state.set_state(GameState.waiting_for_donate_group_selection)
        await state.update_data(user_id=player.user_id)
        logging.info(f"Игрок {player.full_name} ({player.user_id}) выбирает группу для пожертвования.")

# Теперь ваш настоящий обработчик cmd_donate будет просто вызывать эту логику
 
async def cmd_donate(message: Message, state: FSMContext):
    if message.chat.type != ChatType.PRIVATE:
        await message.reply(f"Эту команду можно использовать только в личной переписке с ботом. {FACTION_EMOJIS['town']}")
        return

    # Определяем функцию ответа, специфичную для сообщения
    async def reply_to_message_func(text, reply_markup=None, parse_mode=ParseMode.HTML):
        await message.reply(text, reply_markup=reply_markup, parse_mode=parse_mode)

    await _process_donate_command_logic(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name,
        message.chat.id, # Передаем chat_id, откуда пришла команда
        state,
        reply_to_message_func
    )

# И callback_donate_prompt также будет вызывать эту логику, но с edit_message_text для ответов
 
async def callback_donate_prompt(query: CallbackQuery):
    await query.answer() # Убираем "часики"

    # Определяем функцию ответа, специфичную для сообщения обратного вызова
    async def edit_callback_message_func(text, reply_markup=None, parse_mode=ParseMode.HTML):
        try:
            await query.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except TelegramBadRequest as e:
            logging.warning(f"Не удалось отредактировать сообщение в callback_donate_prompt для пользователя {query.from_user.id}: {e}")
            await bot.send_message(query.from_user.id, text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception as e:
            logging.error(f"Ошибка при редактировании сообщения в callback_donate_prompt для пользователя {query.from_user.id}: {e}", exc_info=True)
            await bot.send_message(query.from_user.id, text, reply_markup=reply_markup, parse_mode=parse_mode)


    # Вам нужно состояние для FSM, поэтому получите его здесь
    state = dp.fsm.get_context(bot=bot, chat_id=query.from_user.id, user_id=query.from_user.id)

    await _process_donate_command_logic(
        query.from_user.id,
        query.from_user.username,
        query.from_user.full_name,
        query.message.chat.id, # Передаем chat_id из исходного сообщения
        state,
        edit_callback_message_func
    )

# --- Хэндлеры для give_dollars и give_diamonds (обновленные) ---
 
async def cmd_give_dollars(message: Message, state: FSMContext):
    with Session() as session:
        sender_player = ensure_player_profile_exists(session, message.from_user.id, message.from_user.username, message.from_user.full_name)
        if sender_player is None: 
                logging.error(f"Failed to get/create player profile for user {message.from_user.id}. Possibly bot's own ID.")
                await message.reply(f"Произошла ошибка при загрузке вашего профиля. Попробуйте позже. {FACTION_EMOJIS['missed']}")
                session.rollback()
                return 
        session.commit()

        amount_str = None
        target_user_id = None
        target_username = None
        # 1. Попытка получить получателя из ответа на сообщение
        if message.reply_to_message:
            target_user_id = message.reply_to_message.from_user.id
            target_username = message.reply_to_message.from_user.username
            # Если это ответ на сообщение, сумма должна быть единственным аргументом
            args = message.text.split(maxsplit=1)
            if len(args) < 2:
                await message.reply("Пожалуйста, укажите сумму Долларов. Пример: <code>/give_dollars 100</code> (ответом на сообщение)", parse_mode=ParseMode.HTML)
                return
            amount_str = args[1]
        else:
            # 2. Получатель указан в аргументах команды
            args = message.text.split(maxsplit=2) # Теперь ожидаем до 3 частей: /cmd, сумма, получатель
            if len(args) < 3:
                await message.reply("Пожалуйста, укажите сумму Долларов и никнейм/ID получателя. Пример: <code>/give_dollars 100 @username</code> или <code>/give_dollars 100 123456789</code>", parse_mode=ParseMode.HTML)
                return
            
            amount_str = args[1]
            target_str = args[2]

            if target_str.startswith('@'):
                target_username = target_str[1:]
            elif target_str.isdigit():
                target_user_id = int(target_str)
            else:
                await message.reply("Неверный формат получателя. Укажите никнейм (с @) или ID пользователя.")
                return
        
        try:
            amount = int(amount_str)
            if amount <= 0:
                await message.reply("Сумма должна быть положительным числом.")
                return

            if sender_player.user_id != BOT_OWNER_ID and sender_player.dollars < amount:
                await message.reply(f"У вас недостаточно Долларов для отправки. Ваш баланс: {sender_player.dollars}{FACTION_EMOJIS['dollars']}.")
                return

            if (target_user_id and target_user_id == message.from_user.id) or \
               (target_username and target_username == message.from_user.username):
                await message.reply("Вы не можете отправить Доллары самому себе.")
                return

            target_player = None
            if target_user_id:
                target_player = session.query(Player).filter_by(user_id=target_user_id, game_id=None).first()
            elif target_username:
                target_player = session.query(Player).filter_by(username=target_username, game_id=None).first()

            if not target_player:
                await message.reply(f"Игрок {'@'+target_username if target_username else f'с ID {target_user_id}'} не найден в базе данных. Он должен сначала хотя бы один раз использовать бота (например, /start).")
                return
            
            # Списываем Доллары у отправителя, если он не владелец бота
            if sender_player.user_id != BOT_OWNER_ID:
                sender_player.dollars -= amount
                session.add(sender_player)
                logging.debug(f"DEBUG: Player {sender_player.full_name}'s dollars updated to {sender_player.dollars}.")
            else:
                logging.debug(f"DEBUG: Owner {sender_player.full_name} attempted to send dollars. No balance deduction or update to DB for owner.")

        # Добавляем Доллары получателю
            target_player.dollars += amount
            session.add(target_player)
            logging.debug(f"DEBUG: Target player {target_player.full_name}'s dollars updated to {target_player.dollars}.")

            session.commit() # <--- Коммит теперь здесь, после всех add и логирования

            # Уведомление отправителю (отправляем в тот чат, откуда пришла команда)
            await bot.send_message(
                chat_id=message.chat.id, # Используем message.chat.id, откуда пришла команда
                text=f"{FACTION_EMOJIS['dollars']} <a href='tg://user?id={sender_player.user_id}'>{sender_player.full_name}</a> успешно отправил <b>{amount} Долларов</b> игроку <a href='tg://user?id={target_player.user_id}'>{target_player.full_name}</a>. {RESULT_EMOJIS['success']}\n"
                f"{'Ваш текущий баланс: ' + str(sender_player.dollars) + FACTION_EMOJIS['dollars'] if sender_player.user_id != BOT_OWNER_ID else 'Как владелец бота, ваши Доллары бесконечны!'}",
                parse_mode=ParseMode.HTML
            )
            logging.info(f"Player {sender_player.full_name} ({sender_player.user_id}) sent {amount} Dollars to {target_player.full_name} ({target_player.user_id}). Owner status: {sender_player.user_id == BOT_OWNER_ID}.")

            # Уведомление получателю (всегда в ЛС)
            try:
                await bot.send_message(
                    chat_id=target_player.user_id,
                    text=f"{FACTION_EMOJIS['dollars']} Игрок <a href='tg://user?id={sender_player.user_id}'>{sender_player.full_name}</a> отправил вам <b>{amount} Долларов</b>! {RESULT_EMOJIS['success']}\n"
                         f"Ваш текущий баланс: {target_player.dollars} Долларов.",
                    parse_mode=ParseMode.HTML
                )
            except TelegramForbiddenError:
                logging.warning(f"Failed to notify {target_player.full_name} ({target_player.user_id}) about receiving dollars (bot blocked).")
            except Exception as e:
                logging.error(f"Error notifying {target_player.full_name} about dollars: {e}", exc_info=True)

        except ValueError:
            await message.reply("Количество должно быть целым числом.")
        except Exception as e:
            logging.error(f"Error in cmd_give_dollars for user {message.from_user.id}: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при обработке команды. Попробуйте снова. {FACTION_EMOJIS['missed']}")
            session.rollback()

 
async def cmd_give_diamonds(message: Message, state: FSMContext):
    with Session() as session:
        sender_player = ensure_player_profile_exists(session, message.from_user.id, message.from_user.username, message.from_user.full_name)
        if sender_player is None: 
                logging.error(f"Failed to get/create player profile for user {message.from_user.id}. Possibly bot's own ID.")
                await message.reply(f"Произошла ошибка при загрузке вашего профиля. Попробуйте позже. {FACTION_EMOJIS['missed']}")
                session.rollback()
                return 
        session.commit()

        amount_str = None
        target_user_id = None
        target_username = None

        # 1. Попытка получить получателя из ответа на сообщение
        if message.reply_to_message:
            target_user_id = message.reply_to_message.from_user.id
            target_username = message.reply_to_message.from_user.username
            # Если это ответ на сообщение, сумма должна быть единственным аргументом
            args = message.text.split(maxsplit=1)
            if len(args) < 2:
                await message.reply("Пожалуйста, укажите сумму Бриллиантов. Пример: <code>/give_diamonds 1</code> (ответом на сообщение)", parse_mode=ParseMode.HTML)
                return
            amount_str = args[1]
        else:
            # 2. Получатель указан в аргументах команды
            args = message.text.split(maxsplit=2) # Теперь ожидаем до 3 частей: /cmd, сумма, получатель
            if len(args) < 3:
                await message.reply("Пожалуйста, укажите сумму Бриллиантов и никнейм/ID получателя. Пример: <code>/give_diamonds 1 @username</code> или <code>/give_diamonds 0.5 123456789</code>", parse_mode=ParseMode.HTML)
                return
            
            amount_str = args[1]
            target_str = args[2]

            if target_str.startswith('@'):
                target_username = target_str[1:]
            elif target_str.isdigit():
                target_user_id = int(target_str)
            else:
                await message.reply("Неверный формат получателя. Укажите никнейм (с @) или ID пользователя.")
                return
        
        try:
            amount = float(amount_str) # Бриллианты могут быть дробными
            if amount <= 0:
                await message.reply("Сумма должна быть положительным числом.")
                return

            # Проверка баланса отправителя, если он не владелец
            if sender_player.user_id != BOT_OWNER_ID and sender_player.diamonds < amount:
                await message.reply(f"У вас недостаточно Бриллиантов для отправки. Ваш баланс: {sender_player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.")
                return

            # Нельзя отправить себе
            if (target_user_id and target_user_id == message.from_user.id) or \
               (target_username and target_username == message.from_user.username):
                await message.reply("Вы не можете отправить Бриллианты самому себе.")
                return

            target_player = None
            if target_user_id:
                target_player = session.query(Player).filter_by(user_id=target_user_id, game_id=None).first()
            elif target_username:
                target_player = session.query(Player).filter_by(username=target_username, game_id=None).first()

            if not target_player:
                await message.reply(f"Игрок {'@'+target_username if target_username else f'с ID {target_user_id}'} не найден в базе данных. Он должен сначала хотя бы один раз использовать бота (например, /start).")
                return

            # Списываем Бриллианты у отправителя, если он не владелец бота
            if sender_player.user_id != BOT_OWNER_ID:
                sender_player.diamonds -= amount
                session.add(sender_player)

            # Добавляем Бриллианты получателю
            target_player.diamonds += amount
            session.add(target_player)
            session.commit()

            # Уведомление отправителю (отправляем в тот чат, откуда пришла команда)
            await bot.send_message(
                chat_id=message.chat.id, # Используем message.chat.id, откуда пришла команда
                text=f"{FACTION_EMOJIS['diamonds']} <a href='tg://user?id={sender_player.user_id}'>{sender_player.full_name}</a> успешно отправил <b>{amount:.2f} Бриллиантов</b> игроку <a href='tg://user?id={target_player.user_id}'>{target_player.full_name}</a>. {RESULT_EMOJIS['success']}\n"
                f"{'Ваш текущий баланс: ' + f'{sender_player.diamonds:.2f}' + FACTION_EMOJIS['diamonds'] if sender_player.user_id != BOT_OWNER_ID else 'Как владелец бота, ваши Бриллианты бесконечны!'}",
                parse_mode=ParseMode.HTML
            )
            logging.info(f"Player {sender_player.full_name} ({sender_player.user_id}) sent {amount:.2f} Diamonds to {target_player.full_name} ({target_player.user_id}). Owner status: {sender_player.user_id == BOT_OWNER_ID}.")

            # Уведомление получателю (всегда в ЛС)
            try:
                await bot.send_message(
                    chat_id=target_player.user_id,
                    text=f"{FACTION_EMOJIS['diamonds']} Игрок <a href='tg://user?id={sender_player.user_id}'>{sender_player.full_name}</a> отправил вам <b>{amount:.2f} Бриллиантов</b>! {RESULT_EMOJIS['success']}\n"
                         f"Ваш текущий баланс: {target_player.diamonds:.2f} Бриллиантов.",
                    parse_mode=ParseMode.HTML
                )
            except TelegramForbiddenError:
                logging.warning(f"Failed to notify {target_player.full_name} ({target_player.user_id}) about receiving diamonds (bot blocked).")
            except Exception as e:
                logging.error(f"Error notifying {target_player.full_name} about diamonds: {e}", exc_info=True)

        except ValueError:
            await message.reply("Количество должно быть числом (целым или дробным).")
        except Exception as e:
            logging.error(f"Error in cmd_give_diamonds for user {message.from_user.id}: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при обработке команды. Попробуйте снова. {FACTION_EMOJIS['missed']}")
            session.rollback()


# --- НОВЫЕ FSM ХЭНДЛЕРЫ ДЛЯ СУММ ПОЖЕРТВОВАНИЙ ---
 
async def process_donate_dollars_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    player_user_id_from_fsm = data.get('user_id') 
    selected_group_id = data.get('selected_group_id') 

    is_owner = (user_id == BOT_OWNER_ID)
    try:
        amount = int(message.text)
        if amount <= 0:
            await message.reply("Сумма пожертвования должна быть положительной.")
            return
        with Session() as session:
            player = session.query(Player).filter_by(user_id=player_user_id_from_fsm, game_id=None).first()
            group = session.get(Group, selected_group_id)

            if not player or player.user_id != user_id or not group:
                await message.reply(f"Произошла ошибка с вашим профилем или выбранной группой. Попробуйте еще раз с /donate. {FACTION_EMOJIS['missed']}")
                await state.clear()
                return

            if not is_owner and player.dollars < amount:
                await message.reply(f"У вас недостаточно Долларов. У вас {player.dollars}{FACTION_EMOJIS['dollars']}.")
                return
            
            # Списываем только у обычных игроков
            if not is_owner:
                player.dollars -= amount
            group.dollars_donated += amount
            
            group_experience_gain = amount * DOLLAR_TO_GROUP_EXP_RATIO
            group.experience += group_experience_gain

            await message.reply(f"Вы успешно пожертвовали {amount} Долларов {FACTION_EMOJIS['dollars']} в Фонд Города <b>{group.name}</b>! Спасибо за ваш вклад. {RESULT_EMOJIS['success']}", parse_mode=ParseMode.HTML)
            logging.info(f"Player {player.full_name} ({player.user_id}) donated {amount} Dollars to group {group.name}.")

            # Проверяем повышение уровня группы
            while group.experience >= (BASE_GROUP_EXP_FOR_LEVEL_UP + (group.level * GROUP_LEVEL_UP_EXP_INCREMENT)):
                group.level += 1
                exp_needed_for_previous_level_up = BASE_GROUP_EXP_FOR_LEVEL_UP + ((group.level - 1) * GROUP_LEVEL_UP_EXP_INCREMENT)
                group.experience -= exp_needed_for_previous_level_up
                
                level_bonus_key = min(group.level, max(GROUP_LEVEL_BONUSES.keys()))
                level_bonus = GROUP_LEVEL_BONUSES.get(level_bonus_key, GROUP_LEVEL_BONUSES.get(max(GROUP_LEVEL_BONUSES.keys())))
                group.bonus_exp_percent = level_bonus['exp_percent']
                group.bonus_dollars_percent = level_bonus['dollars_percent']
                group.bonus_item_chance = level_bonus['item_chance']

                await bot.send_message(group.chat_id,
                                       f"Поздравляем! {FACTION_EMOJIS['town']} Уровень Города <b>{group.name}</b> повышен до <b>{group.level}</b>!\n"
                                       f"Теперь все игроки в этой группе получают: +{group.bonus_exp_percent*100:.0f}% к опыту, +{group.bonus_dollars_percent*100:.0f}% к Долларам.",
                                       parse_mode=ParseMode.HTML)

            session.add(player)
            session.add(group)
            session.commit()
            await state.clear()

    except ValueError:
        await message.reply("Неверный формат. Пожалуйста, введите целое число для Долларов.")
    except Exception as e:
        logging.error(f"Ошибка при обработке пожертвования Долларов от {user_id}: {e}", exc_info=True)
        await message.reply(f"Произошла ошибка при обработке вашего пожертвования. {FACTION_EMOJIS['missed']}")
        session.rollback()
    finally:
        await state.clear()


 
async def process_donate_diamonds_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    player_user_id_from_fsm = data.get('user_id') 
    selected_group_id = data.get('selected_group_id') 

    is_owner = (user_id == BOT_OWNER_ID)

    try:
        amount = float(message.text)
        if amount <= 0:
            await message.reply("Сумма пожертвования должна быть положительной.")
            return

        with Session() as session:
            player = session.query(Player).filter_by(user_id=player_user_id_from_fsm, game_id=None).first() 
            group = session.get(Group, selected_group_id)

            if not player or player.user_id != user_id or not group:
                await message.reply(f"Произошла ошибка с вашим профилем или выбранной группой. Попробуйте еще раз с /donate. {FACTION_EMOJIS['missed']}")
                await state.clear()
                return
            if not is_owner and player.diamonds < amount:
                await message.reply(f"У вас недостаточно Бриллиантов. У вас {player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.")
                return
            
            # Списываем только у обычных игроков
            if not is_owner:
                player.diamonds -= amount
            group.diamonds_donated += amount
            
            group_experience_gain = amount * DIAMOND_TO_GROUP_EXP_RATIO
            group.experience += group_experience_gain

            await message.reply(f"Вы успешно пожертвовали {amount:.2f} Бриллиантов {FACTION_EMOJIS['diamonds']} в Фонд Города <b>{group.name}</b>! Спасибо за ваш ценный вклад. {RESULT_EMOJIS['success']}", parse_mode=ParseMode.HTML)
            logging.info(f"Player {player.full_name} ({player.user_id}) donated {amount:.2f} Diamonds to group {group.name}.")

            # Проверяем повышение уровня группы
            while group.experience >= (BASE_GROUP_EXP_FOR_LEVEL_UP + (group.level * GROUP_LEVEL_UP_EXP_INCREMENT)):
                group.level += 1
                exp_needed_for_previous_level_up = BASE_GROUP_EXP_FOR_LEVEL_UP + ((group.level - 1) * GROUP_LEVEL_UP_EXP_INCREMENT)
                group.experience -= exp_needed_for_previous_level_up
                
                level_bonus_key = min(group.level, max(GROUP_LEVEL_BONUSES.keys()))
                level_bonus = GROUP_LEVEL_BONUSES.get(level_bonus_key, GROUP_LEVEL_BONUSES.get(max(GROUP_LEVEL_BONUSES.keys())))
                group.bonus_exp_percent = level_bonus['exp_percent']
                group.bonus_dollars_percent = level_bonus['dollars_percent']
                group.bonus_item_chance = level_bonus['item_chance']

                await bot.send_message(group.chat_id,
                                       f"Поздравляем! {FACTION_EMOJIS['town']} Уровень Города <b>{group.name}</b> повышен до <b>{group.level}</b>!\n"
                                       f"Теперь все игроки в этой группе получают: +{group.bonus_exp_percent*100:.0f}% к опыту, +{group.bonus_dollars_percent*100:.0f}% к Долларам.",
                                       parse_mode=ParseMode.HTML)

            session.add(player)
            session.add(group)
            session.commit()
            await state.clear()

    except ValueError:
        await message.reply("Неверный формат. Пожалуйста, введите число (можно дробное) для Бриллиантов.")
    except Exception as e:
        logging.error(f"Ошибка при обработке пожертвования Бриллиантов от {user_id}: {e}", exc_info=True)
        await message.reply(f"Произошла ошибка при обработке вашего пожертвования. {FACTION_EMOJIS['missed']}")
        session.rollback()
    finally:
        await state.clear()
# --- КОНЕЦ НОВЫХ FSM ХЭНДЛЕРОВ ДЛЯ СУММ ПОЖЕРТВОВАНИЙ ---


 
async def callback_vote(query: CallbackQuery):
    with Session() as session:
        try:
            if query.message.chat.type != ChatType.PRIVATE:
                await query.answer(f"Это действие можно выполнить только в личной переписке с ботом. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            parts = query.data.split('_')
            game_id = int(parts[1])
            target_player_id = int(parts[2])

            player_in_private = session.query(Player).filter_by(user_id=query.from_user.id, game_id=game_id, is_alive=True).first()
            if not player_in_private:
                await query.answer(f"Вы не участвуете в этой игре или уже мертвы. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            game = session.get(Game, player_in_private.game_id)
            if not game or game.status != 'playing' or game.phase != 'voting':
                await query.answer(f"Сейчас не фаза голосования или игра не активна. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            target_player = session.query(Player).filter_by(id=target_player_id, game_id=game.id, is_alive=True).first()

            if not target_player:
                await query.answer(f"Игрок не найден в игре или уже мертв. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            if player_in_private.id == target_player.id:
                await query.answer(f"Вы не можете голосовать за себя. {FACTION_EMOJIS['missed']}", show_alert=True)
                return
            
            if player_in_private.voted_for_player_id:
                await query.answer("Вы уже проголосовали в этой фазе. Вы можете изменить свой голос.", show_alert=False)
            
            player_in_private.voted_for_player_id = target_player.id
            session.add(player_in_private)
            session.commit()
            logging.info(f"Player {player_in_private.full_name} ({player_in_private.user_id}) voted for {target_player.full_name} in game {game_id}.")

            await bot.send_message(
                chat_id=game.chat_id,
                text=f"{ROLE_EMOJIS['civilian']} <a href='tg://user?id={player_in_private.user_id}'>{player_in_private.full_name}</a> проголосовал за <a href='tg://user?id={target_player.user_id}'>{target_player.full_name}</a>",
                parse_mode=ParseMode.HTML
            )

            await query.answer(f"Вы успешно проголосовали за {target_player.full_name}.", show_alert=False)

            await query.message.edit_text(
                f"Вы проголосовали за <b>{target_player.full_name}</b>. "
                f"Ваш голос учтен. Ждем голосов других игроков. {PHASE_EMOJIS['voting']}",
                reply_markup=None
            )

        except Exception as e:
            logging.error(
                f"Ошибка при обработке голосования через кнопку от {query.from_user.id} в игре {game_id if 'game_id' in locals() else 'Unknown'}: {e}", exc_info=True)
            await query.answer(f"Произошла ошибка при голосовании. Попробуйте позже. {FACTION_EMOJIS['missed']}", show_alert=True)
            session.rollback()


 
async def callback_lynch_vote(query: CallbackQuery):
    with Session() as session:
        try:
            parts = query.data.split('_')
            action = parts[1]
            game_id = int(parts[2])

            game = session.get(Game, game_id)
            if not game or game.status != 'playing' or game.phase != 'lynch_vote' or not game.voted_for_player_id:
                await query.answer(f"Сейчас не фаза голосования за казнь, игра не активна или нет цели. {PHASE_EMOJIS['voting']}",
                                   show_alert=True)
                return

            voter_user_id = query.from_user.id
            voter = session.query(Player).filter_by(user_id=voter_user_id, game_id=game.id, is_alive=True).first()
            if not voter:
                await query.answer(f"Вы не участвуете в этой игре или уже мертвы. {PHASE_EMOJIS['death']}", show_alert=True)
                return
            
            voters_list = [int(x) for x in game.lynch_voters.split(',') if x]
            if voter.user_id in voters_list:
                await query.answer("Вы уже проголосовали в этой фазе казнить/помиловать.", show_alert=True)
                return

            if action == 'like':
                game.lynch_vote_likes += 1
                confirmation_text_player = f"Вы проголосовали <b>ЗА</b> казнь. {FACTION_EMOJIS['mafia']}"
            else:
                game.lynch_vote_dislikes += 1
                confirmation_text_player = f"Вы проголосовали <b>ПРОТИВ</b> казни. {FACTION_EMOJIS['town']}"
            
            voters_list.append(voter.user_id)
            game.lynch_voters = ",".join(map(str, voters_list))

            session.add(game)
            session.commit()
            logging.info(f"Player{voter.full_name} ({voter.user_id}) voted {'FOR' if action == 'like' else 'AGAINST'} lynch in game {game_id}.")

            executed_player = session.get(Player, game.voted_for_player_id)
            if executed_player and game.lynch_message_id:
                keyboard_lynch = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=f"КАЗНИТЬ {FACTION_EMOJIS['mafia']}", callback_data=f"lynch_like_{game.id}_{executed_player.id}")],
                    [InlineKeyboardButton(text=f"ПОМИЛОВАТЬ {FACTION_EMOJIS['town']}", callback_data=f"lynch_dislike_{game.id}_{executed_player.id}")]
                ])

                updated_text = (
                    f"{PHASE_EMOJIS['lynch_vote']} <b>Фаза суда!</b> Жители, вы проголосовали за <a href='tg://user?id={executed_player.user_id}'><b>{executed_player.full_name}</b></a>.\n"
                    f"У вас есть {PHASE_DURATIONS['lynch_vote']} секунд, чтобы решить: <b>казнить его или помиловать?</b>\n"
                    f"<i>(Если никто не проголосует, его казнят. Если будет ничья — помилуют)</i>\n\n"
                    f"{FACTION_EMOJIS['mafia']} За казнь: <b>{game.lynch_vote_likes}</b> | {FACTION_EMOJIS['town']} Против казни: <b>{game.lynch_vote_dislikes}</b>"
                )
                
                try:
                    await bot.edit_message_text(
                        chat_id=game.chat_id,
                        message_id=game.lynch_message_id,
                        text=updated_text,
                        reply_markup=keyboard_lynch,
                        parse_mode=ParseMode.HTML
                    )
                    logging.info(f"Updated lynch vote counts in chat {game.chat_id}.")
                except (TelegramBadRequest, TelegramForbiddenError) as e:
                    logging.warning(f"Не удалось обновить сообщение с голосованием за казнь (ID: {game.lynch_message_id}): {e}")
                except Exception as e:
                    logging.error(f"Неизвестная ошибка при редактировании сообщения с голосованием за казнь: {e}")
                    try:
                        await bot.send_message(
                            chat_id=voter_user_id,
                            text=f"{confirmation_text_player} Ваш голос учтен. "
                                f"Ждем голосов остальных. {PHASE_EMOJIS['voting']}",
                            parse_mode=ParseMode.HTML
                        )
                    except TelegramForbiddenError: 
                        logging.warning(f"Бот заблокирован игроком {voter.full_name}: не удалось отправить подтверждение голосования.")
                        await query.answer("Ваш голос учтен, но не удалось отправить подтверждение в ЛС.", show_alert=True)
                    except Exception as e_inner: 
                        logging.warning(f"Не удалось отправить подтверждение голосования игроку {voter.full_name} ({voter_user_id}) в ЛС: {e_inner}")
                        await query.answer("Ваш голос учтен, но не удалось отправить подтверждение в ЛС.", show_alert=True)

                await query.answer("Ваш голос учтен.", show_alert=False) 
        except Exception as e:
            logging.error(f"Ошибка при обработке голосования за казнь от {query.from_user.id} в игре {game_id if 'game_id' in locals() else 'Unknown'}: {e}",
                          exc_info=True)
            await query.answer(f"Произошла ошибка при голосовании за казнь. Попробуйте позже. {FACTION_EMOJIS['missed']}", show_alert=True)
            session.rollback()


async def callback_mafia_kill(query: CallbackQuery):
    with Session() as session:
        try:
            if query.message.chat.type != ChatType.PRIVATE:
                await query.answer(f"Это действие можно выполнить только в личной переписке с ботом. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            parts = query.data.split('_')
            game_id = int(parts[2])
            target_player_id = int(parts[3])

            player = session.query(Player).filter_by(user_id=query.from_user.id, game_id=game_id, is_alive=True).first()
            if not player or player.game_id != game_id:
                await query.answer(f"Вы не участвуете в этой игре или уже мертвы. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            game = session.get(Game, player.game_id)
            if not game or game.status != 'playing' or game.phase != 'night' or player.role not in ['mafia', 'don']:
                await query.answer(f"Сейчас не ваша очередь действовать или вы не Мафия. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            target_player = session.query(Player).filter_by(id=target_player_id, game_id=game.id, is_alive=True).first()

            if not target_player:
                await query.answer(f"Цель не найдена в игре или уже мертва. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            if player.id == target_player.id:
                await query.answer(f"Мафия не может убить себя. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            if target_player.role in ['mafia', 'don']:
                await query.answer(f"Мафия не может убить другого мафиози. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            player.night_action_target_id = target_player.id
            session.add(player)
            session.commit()
            logging.info(f"Mafia player {player.full_name} ({player.user_id}) chose to kill {target_player.full_name} in game {game_id}.")

            await query.message.edit_text(
                f"Твой выбор: <b>{target_player.full_name}</b> {ROLE_EMOJIS[player.role]}",
                reply_markup=None
            )
            await query.answer(f"Вы выбрали убить {target_player.full_name}.", show_alert=False)

            mafia_allies_and_self = session.query(Player).filter(
                Player.game_id == game.id,
                Player.is_alive == True,
                Player.role.in_(['mafia', 'don'])
            ).all()

            notification_message = f"{ROLE_EMOJIS[player.role]} <b>{player.full_name}</b> Сделал выбор: {target_player.full_name}"
            for ally in mafia_allies_and_self:
                if ally.user_id != player.user_id:
                    try:
                        await bot.send_message(chat_id=ally.user_id, text=notification_message, parse_mode=ParseMode.HTML)
                    except TelegramForbiddenError:
                        logging.warning(f"Бот заблокирован союзником {ally.full_name}: не удалось отправить уведомление о выборе.")
                    except Exception as e:
                        logging.error(f"Ошибка при отправке уведомления о выборе союзнику {ally.full_name}: {e}", exc_info=True)

        except Exception as e:
            logging.error(f"Error in mafia_kill for {query.from_user.id}: {e}", exc_info=True)
            await query.answer(f"Произошла ошибка. Попробуйте позже. {FACTION_EMOJIS['missed']}", show_alert=True)
            session.rollback()


async def callback_doctor_heal(query: CallbackQuery):
    with Session() as session:
        try:
            if query.message.chat.type != ChatType.PRIVATE:
                await query.answer(f"Это действие можно выполнить только в личной переписке с ботом. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            parts = query.data.split('_')
            game_id = int(parts[2])
            target_player_id = int(parts[3])

            player = session.query(Player).filter_by(user_id=query.from_user.id, game_id=game_id, is_alive=True).first()
            if not player or player.game_id != game_id:
                await query.answer(f"Вы не участвуете в этой игре или уже мертвы. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            game = session.get(Game, player.game_id)
            if not game or game.status != 'playing' or game.phase != 'night' or player.role != 'doctor':
                await query.answer(f"Сейчас не ваша очередь действовать или вы не Доктор. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            target_player = session.query(Player).filter_by(id=target_player_id, game_id=game.id, is_alive=True).first()

            if not target_player:
                await query.answer(f"Цель не найдена в игре или уже мертва. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            player.night_action_target_id = target_player.id
            session.add(player)
            session.commit()
            logging.info(f"Doctor player {player.full_name} ({player.user_id}) chose to heal {target_player.full_name} in game {game_id}.")

            await query.message.edit_text(
                f"Твой выбор: <b>{target_player.full_name}</b> {ROLE_EMOJIS[player.role]}",
                reply_markup=None
            )
            await query.answer(f"Вы выбрали вылечить {target_player.full_name}.", show_alert=False)
            
            state = dp.fsm.get_context(bot=bot, chat_id=player.user_id, user_id=player.user_id)
            await state.clear()


        except Exception as e:
            logging.error(
                f"Ошибка при обработке действия Доктора от {query.from_user.id} в игре {game_id if 'game_id' in locals() else 'Unknown'}: {e}",
                exc_info=True)
            await query.answer(f"Произошла ошибка. Попробуйте позже. {FACTION_EMOJIS['missed']}", show_alert=True)
            session.rollback()


async def callback_commissioner_check(query: CallbackQuery):
    with Session() as session:
        try:
            if query.message.chat.type != ChatType.PRIVATE:
                await query.answer(f"Это действие можно выполнить только в личной переписке с ботом. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            parts = query.data.split('_')
            game_id = int(parts[2])
            target_player_id = int(parts[3])

            player = session.query(Player).filter_by(user_id=query.from_user.id, game_id=game_id, is_alive=True).first()
            if not player or player.game_id != game_id:
                await query.answer(f"Вы не участвуете в этой игре или уже мертвы. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            game = session.get(Game, player.game_id)
            if not game or game.status != 'playing' or game.phase != 'night' or player.role != 'commissioner':
                await query.answer(f"Сейчас не ваша очередь действовать или вы не Комиссар. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            target_player = session.query(Player).filter_by(id=target_player_id, game_id=game.id, is_alive=True).first()

            if not target_player:
                await query.answer(f"Цель не найдена в игре или уже мертва. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            if player.id == target_player.id:
                await query.answer(f"Вы не можете проверять себя. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            player.night_action_target_id = target_player.id
            session.add(player)
            session.commit()
            logging.info(f"Commissioner player {player.full_name} ({player.user_id}) chose to check {target_player.full_name} in game {game_id}.")

            is_mafia_faction = target_player.role in ['mafia', 'don']
            faction_status = "Мафией" if is_mafia_faction else "Мирным жителем"
            role_emoji_faction = FACTION_EMOJIS['mafia'] if is_mafia_faction else FACTION_EMOJIS['town']

            await bot.send_message(
                chat_id=player.user_id,
                text=f"Ваша проверка завершена. {ROLE_EMOJIS['commissioner']} Игрок <b>{target_player.full_name}</b> является {role_emoji_faction} <b>{faction_status}</b>. {FACTION_EMOJIS['town']}"
            )

            await query.message.edit_text(
                f"Твой выбор: <b>{target_player.full_name}</b> {ROLE_EMOJIS[player.role]}",
                reply_markup=None
            )
            await query.answer(f"Вы выбрали проверить {target_player.full_name}.", show_alert=False)
            
            state = dp.fsm.get_context(bot=bot, chat_id=player.user_id, user_id=player.user_id)
            await state.clear()


        except Exception as e:
            logging.error(
                f"Ошибка при обработке действия Комиссара от {query.from_user.id} в игре {game_id if 'game_id' in locals() else 'Unknown'}: {e}",
                exc_info=True)
            await query.answer(f"Произошла ошибка. Попробуйте позже. {FACTION_EMOJIS['missed']}", show_alert=True)
            session.rollback()

  
async def callback_maniac_kill(query: CallbackQuery):
    with Session() as session:
        try:
            if query.message.chat.type != ChatType.PRIVATE:
                await query.answer(f"Это действие можно выполнить только в личной переписке с ботом. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            parts = query.data.split('_')
            game_id = int(parts[2])
            target_player_id = int(parts[3])

            player = session.query(Player).filter_by(user_id=query.from_user.id, game_id=game_id, is_alive=True).first()
            if not player or player.game_id != game_id:
                await query.answer(f"Вы не участвуете в этой игре или уже мертвы. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            game = session.get(Game, player.game_id)
            if not game or game.status != 'playing' or game.phase != 'night' or player.role != 'maniac':
                await query.answer(f"Сейчас не ваша очередь действовать или вы не Маньяк. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            target_player = session.query(Player).filter_by(id=target_player_id, game_id=game.id, is_alive=True).first()

            if not target_player:
                await query.answer(f"Цель не найдена в игре или уже мертва. {PHASE_EMOJIS['death']}", show_alert=True)
                return

            if player.id == target_player.id:
                await query.answer(f"Вы не можете убить себя. {FACTION_EMOJIS['missed']}", show_alert=True)
                return

            player.night_action_target_id = target_player.id
            session.add(player)
            session.commit()
            logging.info(f"Maniac player {player.full_name} ({player.user_id}) chose to kill {target_player.full_name} in game {game_id}.")

            await query.message.edit_text(
                f"Твой выбор: <b>{target_player.full_name}</b> {ROLE_EMOJIS[player.role]}",
                reply_markup=None
            )
            await query.answer(f"Вы выбрали убить {target_player.full_name}.", show_alert=False)

            state = dp.fsm.get_context(bot=bot, chat_id=player.user_id, user_id=player.user_id)
            await state.clear()


        except Exception as e:
            logging.error(f"Error in maniac_kill for {query.from_user.id}: {e}", exc_info=True)
            await query.answer(f"Произошла ошибка. Попробуйте позже. {FACTION_EMOJIS['missed']}", show_alert=True)
            session.rollback()
 
async def callback_select_frame_prompt(query: CallbackQuery, state: FSMContext):
    await query.answer()
    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            return

        unlocked_frames = json.loads(player.unlocked_frames)
            
        keyboard_buttons = []
        for frame_id, frame_data in CUSTOM_FRAMES.items():
            is_selected = (frame_id == player.selected_frame)
            is_unlocked = (frame_id in unlocked_frames)
                
            button_text = f"{frame_data['name_ru']}"
            if is_selected:
                button_text += " (Выбрано)"
            elif not is_unlocked:
                price_dollars = frame_data.get('price_dollars', 0)
                price_diamonds = frame_data.get('price_diamonds', 0.0)
                if price_dollars > 0:
                    button_text += f" ({price_dollars}{FACTION_EMOJIS['dollars']})"
                if price_diamonds > 0:
                    button_text += f" ({price_diamonds:.1f}{FACTION_EMOJIS['diamonds']})"
                
            keyboard_buttons.append(
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"preview_frame_{frame_id}" # МЕНЯЕМ COLLBACK_DATA НА ПРЕДПРОСМОТР
                )
            )

        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            keyboard_buttons[i:i + 1] for i in range(0, len(keyboard_buttons), 1)
        ] + [[InlineKeyboardButton(text="?? Назад в профиль", callback_data="back_to_profile")]]) # КНОПКА НАЗАД
            
        is_owner = (query.from_user.id == BOT_OWNER_ID)
        dollars_display = "?" if is_owner else str(player.dollars)
        diamonds_display = "?" if is_owner else f"{player.diamonds:.2f}"

        await query.message.edit_text(
            f"Выберите рамку для вашего профиля. У вас {dollars_display}{FACTION_EMOJIS['dollars']} и {diamonds_display}{FACTION_EMOJIS['diamonds']}.",
            reply_markup=inline_keyboard,
            parse_mode=ParseMode.HTML
        )
        await state.set_state(GameState.waiting_for_frame_selection) # Состояние выбора (чтобы ловить preview_frame_)

 
async def callback_select_frame(query: CallbackQuery, state: FSMContext):
    await query.answer()
    frame_id = query.data.split('_')[2]

    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
        
        if frame_id not in CUSTOM_FRAMES:
            await query.message.edit_text(f"Ошибка: Неизвестная рамка. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
        
        frame_data = CUSTOM_FRAMES[frame_id]
        unlocked_frames = json.loads(player.unlocked_frames)
        
        if frame_id not in unlocked_frames:
            # Попытка купить рамку
            price_dollars = frame_data.get('price_dollars', 0)
            price_diamonds = frame_data.get('price_diamonds', 0.0)

            can_afford = True
            if price_dollars > 0 and player.dollars < price_dollars and player.user_id != BOT_OWNER_ID:
                can_afford = False
            if price_diamonds > 0 and player.diamonds < price_diamonds and player.user_id != BOT_OWNER_ID:
                can_afford = False

            if not can_afford:
                await query.message.edit_text(
                    f"У вас недостаточно средств, чтобы купить рамку <b>{frame_data['name_ru']}</b>. "
                    f"Требуется: {price_dollars}{FACTION_EMOJIS['dollars']} и {price_diamonds:.1f}{FACTION_EMOJIS['diamonds']}.\n"
                    f"Ваш баланс: {player.dollars}{FACTION_EMOJIS['dollars']} и {player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                await state.clear()
                return

            # Списываем средства
            if player.user_id != BOT_OWNER_ID:
                player.dollars -= price_dollars
                player.diamonds -= price_diamonds
            
            unlocked_frames.append(frame_id)
            player.unlocked_frames = json.dumps(unlocked_frames)
            session.add(player)
            session.commit()
            
            # В callback_select_frame:
            current_dollars_display = "?" if player.user_id == BOT_OWNER_ID else str(player.dollars)
            current_diamonds_display = "?" if player.user_id == BOT_OWNER_ID else f"{player.diamonds:.2f}"
            await query.message.edit_text(
                f"Вы купили и выбрали рамку <b>{frame_data['name_ru']}</b>! {RESULT_EMOJIS['success']}\n"
                f"Текущий баланс: {current_dollars_display}{FACTION_EMOJIS['dollars']}, {current_diamonds_display}{FACTION_EMOJIS['diamonds']}.",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
            logging.info(f"Player {player.full_name} ({player.user_id}) bought and selected frame '{frame_id}'.")
            await state.clear()
            return

        # Если рамка уже разблокирована, просто выбираем её
        player.selected_frame = frame_id
        session.add(player)
        session.commit()
        
        await query.message.edit_text(
            f"Вы выбрали рамку <b>{frame_data['name_ru']}</b>. {RESULT_EMOJIS['success']}\n"
            f"Проверьте свой профиль с помощью /profile.",
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
        logging.info(f"Player {player.full_name} ({player.user_id}) selected frame '{frame_id}'.")
        await state.clear()

  
async def callback_preview_frame(query: CallbackQuery, state: FSMContext):
    await query.answer()
    frame_id = query.data.split('_')[2]

    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
            
        if frame_id not in CUSTOM_FRAMES:
            await query.message.edit_text(f"Ошибка: Неизвестная рамка. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
        frame_data = CUSTOM_FRAMES[frame_id]
        unlocked_frames = json.loads(player.unlocked_frames)
            
        is_unlocked = (frame_id in unlocked_frames)
        is_selected = (frame_id == player.selected_frame)

        action_text = ""
        action_button_text = ""
        cost_text = ""

        if is_selected:
            action_text = "Эта рамка уже выбрана."
            action_button_text = "? Выбрано"
        elif is_unlocked:
            action_text = f"Вы можете выбрать рамку <b>{frame_data['name_ru']}</b>."
            action_button_text = "Выбрать эту рамку"
        else:
            price_dollars = frame_data.get('price_dollars', 0)
            price_diamonds = frame_data.get('price_diamonds', 0.0)
            cost_text = ""
            if price_dollars > 0:
                cost_text += f" {price_dollars}{FACTION_EMOJIS['dollars']}"
            if price_diamonds > 0:
                cost_text += f" {price_diamonds:.1f}{FACTION_EMOJIS['diamonds']}"
                
            action_text = f"Стоимость рамки <b>{frame_data['name_ru']}</b>: {cost_text}."
            action_button_text = f"Купить и выбрать ({cost_text.strip()})"
                
            # Формируем предпросмотр профиля
            # Временно устанавливаем рамку для предпросмотра
        original_selected_frame = player.selected_frame
        player.selected_frame = frame_id
            
            # Используем вспомогательную функцию для форматирования профиля, но БЕЗ кнопок
        preview_text_parts = await _format_player_profile_text(player, session) # Нужно будет создать эту вспомогательную функцию
            
        player.selected_frame = original_selected_frame # Возвращаем оригинальную рамку

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=action_button_text, callback_data=f"confirm_frame_action_{frame_id}", disable_web_page_preview=True)] if not is_selected else [],
            [InlineKeyboardButton(text="?? Назад к рамкам", callback_data="back_to_frames_list")]
        ])
            
        await query.message.edit_text(
            f"{preview_text_parts}\n\n"
            f"{FACTION_EMOJIS['info']} {action_text}\n"
            f"Ваш баланс: {player.dollars}{FACTION_EMOJIS['dollars']}, {player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        await state.set_state(GameState.waiting_for_frame_preview_action)
        await state.update_data(current_frame_id=frame_id) # Сохраняем ID рамки для следующего шага
 
async def callback_confirm_frame_action(query: CallbackQuery, state: FSMContext):
    await query.answer()
    frame_id = query.data.split('_')[3] # confirm_frame_action_frame_id

    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
            
        frame_data = CUSTOM_FRAMES[frame_id]
        unlocked_frames = json.loads(player.unlocked_frames)
            
            # Логика покупки/выбора
        if frame_id not in unlocked_frames:
                # Покупка
            price_dollars = frame_data.get('price_dollars', 0)
            price_diamonds = frame_data.get('price_diamonds', 0.0)

            can_afford = True
            if price_dollars > 0 and player.dollars < price_dollars and player.user_id != BOT_OWNER_ID:
                    can_afford = False
            if price_diamonds > 0 and player.diamonds < price_diamonds and player.user_id != BOT_OWNER_ID:
                    can_afford = False

            if not can_afford:
                await query.message.edit_text(
                    f"У вас недостаточно средств, чтобы купить рамку <b>{frame_data['name_ru']}</b>. "
                    f"Требуется: {price_dollars}{FACTION_EMOJIS['dollars']} и {price_diamonds:.1f}{FACTION_EMOJIS['diamonds']}.\n"
                    f"Ваш баланс: {player.dollars}{FACTION_EMOJIS['dollars']} и {player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="?? Назад к рамкам", callback_data="back_to_frames_list")]])
                )
                await state.clear() # Можно вернуться к списку, а не сразу очищать FSM
                return

                # Списываем средства
            if player.user_id != BOT_OWNER_ID:
                player.dollars -= price_dollars
                player.diamonds -= price_diamonds
                
            unlocked_frames.append(frame_id)
            player.unlocked_frames = json.dumps(unlocked_frames)
            session.add(player)
            session.commit()
                
            current_dollars_display = "?" if player.user_id == BOT_OWNER_ID else str(player.dollars)
            current_diamonds_display = "?" if player.user_id == BOT_OWNER_ID else f"{player.diamonds:.2f}"
                
            await query.message.edit_text(
                f"Вы купили и выбрали рамку <b>{frame_data['name_ru']}</b>! {RESULT_EMOJIS['success']}\n"
                f"Текущий баланс: {current_dollars_display}{FACTION_EMOJIS['dollars']}, {current_diamonds_display}{FACTION_EMOJIS['diamonds']}.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="?? Назад к рамкам", callback_data="back_to_frames_list")]])
                )
            logging.info(f"Player {player.full_name} ({player.user_id}) bought and selected frame '{frame_id}'.")
            await state.clear() # Можно вернуться к списку, а не сразу очищать FSM
            return

            # Если рамка уже разблокирована, просто выбираем её
        player.selected_frame = frame_id
        session.add(player)
        session.commit()
            
        await query.message.edit_text(
            f"Вы выбрали рамку <b>{frame_data['name_ru']}</b>. {RESULT_EMOJIS['success']}\n"
            f"Проверьте свой профиль с помощью /profile.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="?? Назад к рамкам", callback_data="back_to_frames_list")]])
        )
        logging.info(f"Player {player.full_name} ({player.user_id}) selected frame '{frame_id}'.")
        await state.clear() # Можно вернуться к списку, а не сразу очищать FSM

# --- Хэндлеры для кастомизации профиля (Титулы) ---
 
async def callback_select_title_prompt(query: CallbackQuery, state: FSMContext):
    await query.answer()
    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            return

        unlocked_titles = json.loads(player.unlocked_titles)
        
        keyboard_buttons = []
        for title_id, title_data in CUSTOM_TITLES.items():
            is_selected = (title_id == player.selected_title)
            is_unlocked = (title_id in unlocked_titles)
            
            button_text = f"{title_data['emoji']} {title_data['name_ru']}" # Убрал лишние '???'
            if is_selected:
                button_text += " (Выбрано)"
            elif not is_unlocked:
                price_dollars = title_data.get('price_dollars', 0)
                price_diamonds = title_data.get('price_diamonds', 0.0)
                if price_dollars > 0:
                    button_text += f" ({price_dollars}{FACTION_EMOJIS['dollars']})"
                if price_diamonds > 0:
                    button_text += f" ({price_diamonds:.1f}{FACTION_EMOJIS['diamonds']})"
            
            keyboard_buttons.append(
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"preview_title_{title_id}" # ИЗМЕНЕНО НА ПРЕДПРОСМОТР
                )
            )

        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            keyboard_buttons[i:i + 1] for i in range(0, len(keyboard_buttons), 1)
        ] + [[InlineKeyboardButton(text="?? Назад в профиль", callback_data="back_to_profile")]]) # ДОБАВЛЕНА КНОПКА НАЗАД
        
        is_owner = (query.from_user.id == BOT_OWNER_ID)
        dollars_display = "?" if is_owner else str(player.dollars)
        diamonds_display = "?" if is_owner else f"{player.diamonds:.2f}"

        await query.message.edit_text(
            f"Выберите титул для вашего профиля. У вас {dollars_display}{FACTION_EMOJIS['dollars']} и {diamonds_display}{FACTION_EMOJIS['diamonds']}.",
            reply_markup=inline_keyboard,
            parse_mode=ParseMode.HTML
        )
        await state.set_state(GameState.waiting_for_title_selection) # Состояние выбора (чтобы ловить preview_title_)
async def _format_player_profile_text(player_data: Player, session) -> str:
    """
    Формирует только текстовую часть профиля игрока для предпросмотра.
    player_data здесь - это ГЛОБАЛЬНЫЙ ПРОФИЛЬ (game_id=None).
    """
    is_owner = (player_data.user_id == BOT_OWNER_ID)

    win_rate = 0.0
    if player_data.total_games and player_data.total_games > 0:
        win_rate = (player_data.total_wins / player_data.total_games) * 100
    win_rate = min(win_rate, 100.0)

    total_deaths = player_data.total_deaths or 0
    total_kills = player_data.total_kills or 0
        
    kd_ratio_text = "0.00"
    if total_deaths == 0:
        if total_kills > 0:
            kd_ratio_text = "?" # Символ бесконечности
        else:
            kd_ratio_text = "0.00"
    else:
        kd_ratio_text = f"{(total_kills / total_deaths):.2f}"
        
    exp_to_next_level_val = get_exp_for_next_level(player_data.level)
    remaining_exp = exp_to_next_level_val - player_data.experience

    gender_emoji = GENDER_EMOJIS.get(player_data.gender, "?")
    gender_name = ROLE_NAMES_RU.get(player_data.gender, "Не указан")

    selected_frame_key = player_data.selected_frame if player_data.selected_frame in CUSTOM_FRAMES else "default"
    frame = CUSTOM_FRAMES[selected_frame_key]
    divider_top = frame["top"]
    divider_middle = frame["middle"]
    divider_bottom = frame["bottom"]

    group_info_text = ""
    if player_data.last_played_group_id:
        group_obj = session.get(Group, player_data.last_played_group_id)
        if group_obj:
            group_info_text = f"<b>{format_group_info(group_obj)}</b>"
        
    player_title_text = ""
    selected_title_key = player_data.selected_title if player_data.selected_title in CUSTOM_TITLES else "default"
    title_info = CUSTOM_TITLES[selected_title_key]
    player_title_text = f"{title_info['emoji']} [{title_info['name_ru']}] "

    dollars_display = "?" if is_owner else str(player_data.dollars)
    diamonds_display = "?" if is_owner else f"{player_data.diamonds:.2f}"
        
    profile_text = (
        f"<code>{divider_top}</code>\n"
        f"?? Профиль игрока: {player_title_text}<b>{player_data.full_name}</b>\n"
        f"<code>{divider_middle}</code>\n"
        f"?? ID: <code>{player_data.user_id}</code>\n"
        f"?? Ник: @{player_data.username if player_data.username else 'нет'}\n"
        f"?? Пол: {gender_emoji} {gender_name}\n"
        f"<code>{divider_middle}</code>\n"
        f"{group_info_text}"
        f"{FACTION_EMOJIS['dollars']} Долларов: {dollars_display}\n" 
        f"{FACTION_EMOJIS['diamonds']} Бриллиантов: {diamonds_display}\n" 
        f"<code>{divider_middle}</code>\n"
        f"?? Уровень: {player_data.level}\n"
        f"? Опыт: {player_data.experience:.0f} (до след. ур: {max(0, remaining_exp):.0f})\n"
        f"<code>{divider_middle}</code>\n"
        f"?? Количество побед: {player_data.total_wins}\n"
        f"?? Общее количество игр: {player_data.total_games}\n"
        f"?? Win Rate (WR): {win_rate:.2f}%\n"
        f"?? Kill/Death (KD): {kd_ratio_text}\n"
        f"<code>{divider_bottom}</code>"
    )
    return profile_text
 
async def callback_preview_title(query: CallbackQuery, state: FSMContext):
    await query.answer()
    title_id = query.data.split('_')[2]

    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
            
        if title_id not in CUSTOM_TITLES:
            await query.message.edit_text(f"Ошибка: Неизвестный титул. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return

        title_data = CUSTOM_TITLES[title_id]
        unlocked_titles = json.loads(player.unlocked_titles)
            
        is_unlocked = (title_id in unlocked_titles)
        is_selected = (title_id == player.selected_title)

        action_text = ""
        action_button_text = ""
        cost_text = ""

        if is_selected:
            action_text = "Этот титул уже выбран."
            action_button_text = " Выбрано"
        elif is_unlocked:
            action_text = f"Вы можете выбрать титул <b>{title_data['name_ru']}</b>."
            action_button_text = "Выбрать этот титул"
        else:
            price_dollars = title_data.get('price_dollars', 0)
            price_diamonds = title_data.get('price_diamonds', 0.0)
            cost_text = ""
            if price_dollars > 0:
                cost_text += f" {price_dollars}{FACTION_EMOJIS['dollars']}"
            if price_diamonds > 0:
                cost_text += f" {price_diamonds:.1f}{FACTION_EMOJIS['diamonds']}"
            
            action_text = f"Стоимость титула <b>{title_data['name_ru']}</b>: {cost_text}."
            action_button_text = f"Купить и выбрать ({cost_text.strip()})"
                
        # Формируем предпросмотр профиля
        # Временно устанавливаем титул для предпросмотра
        original_selected_title = player.selected_title
        player.selected_title = title_id
        
        # Используем вспомогательную функцию для форматирования профиля, но БЕЗ кнопок
        preview_text_parts = await _format_player_profile_text(player, session)
        
        player.selected_title = original_selected_title # Возвращаем оригинальный титул

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=action_button_text, callback_data=f"confirm_title_action_{title_id}", disable_web_page_preview=True)] if not is_selected else [],
            [InlineKeyboardButton(text="?? Назад к титулам", callback_data="back_to_titles_list")]
        ])
        
        await query.message.edit_text(
            f"{preview_text_parts}\n\n"
            f"{FACTION_EMOJIS['info']} {action_text}\n"
            f"Ваш баланс: {player.dollars}{FACTION_EMOJIS['dollars']}, {player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        await state.set_state(GameState.waiting_for_title_preview_action)
        await state.update_data(current_title_id=title_id) # Сохраняем ID титула для следующего шага
  
async def callback_confirm_title_action(query: CallbackQuery, state: FSMContext):
    await query.answer()
    title_id = query.data.split('_')[3] # confirm_title_action_title_id

    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
            
        title_data = CUSTOM_TITLES[title_id]
        unlocked_titles = json.loads(player.unlocked_titles)
            
        # Логика покупки/выбора
        if title_id not in unlocked_titles:
            # Покупка
            price_dollars = title_data.get('price_dollars', 0)
            price_diamonds = title_data.get('price_diamonds', 0.0)

            can_afford = True
            if price_dollars > 0 and player.dollars < price_dollars and player.user_id != BOT_OWNER_ID:
                can_afford = False
            if price_diamonds > 0 and player.diamonds < price_diamonds and player.user_id != BOT_OWNER_ID:
                can_afford = False

            if not can_afford:
                await query.message.edit_text(
                    f"У вас недостаточно средств, чтобы купить титул <b>{title_data['name_ru']}</b>. "
                    f"Требуется: {price_dollars}{FACTION_EMOJIS['dollars']} и {price_diamonds:.1f}{FACTION_EMOJIS['diamonds']}.\n"
                    f"Ваш баланс: {player.dollars}{FACTION_EMOJIS['dollars']} и {player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="?? Назад к титулам", callback_data="back_to_titles_list")]])
                )
                # await state.clear() # Можно вернуться к списку, а не сразу очищать FSM
                return

            # Списываем средства
            if player.user_id != BOT_OWNER_ID:
                player.dollars -= price_dollars
                player.diamonds -= price_diamonds
            
            unlocked_titles.append(title_id)
            player.unlocked_titles = json.dumps(unlocked_titles)
            session.add(player)
            session.commit()
            
            current_dollars_display = "?" if player.user_id == BOT_OWNER_ID else str(player.dollars)
            current_diamonds_display = "?" if player.user_id == BOT_OWNER_ID else f"{player.diamonds:.2f}"
            
            await query.message.edit_text(
                f"Вы купили и выбрали титул <b>{title_data['name_ru']}</b>! {RESULT_EMOJIS['success']}\n"
                f"Текущий баланс: {current_dollars_display}{FACTION_EMOJIS['dollars']}, {current_diamonds_display}{FACTION_EMOJIS['diamonds']}.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="?? Назад к титулам", callback_data="back_to_titles_list")]])
            )
            logging.info(f"Player {player.full_name} ({player.user_id}) bought and selected title '{title_id}'.")
            # await state.clear() # Можно вернуться к списку, а не сразу очищать FSM
            return

        # Если титул уже разблокирован, просто выбираем его
        player.selected_title = title_id
        session.add(player)
        session.commit()
        
        await query.message.edit_text(
            f"Вы выбрали титул <b>{title_data['name_ru']}</b>. {RESULT_EMOJIS['success']}\n"
            f"Проверьте свой профиль с помощью /profile.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="?? Назад к титулам", callback_data="back_to_titles_list")]])
        )
        logging.info(f"Player {player.full_name} ({player.user_id}) selected title '{title_id}'.")
        # await state.clear() # Можно вернуться к списку, а не сразу очищать FSM    
 
async def callback_back_to_profile(query: CallbackQuery, state: FSMContext):
    await query.answer()
    with Session() as session:
        player_data = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        await display_player_profile(query.message, player_data) # query.message - это объект Message, который будет изменен
        await state.clear()

 
async def callback_back_to_frames_list(query: CallbackQuery, state: FSMContext):
    await query.answer()
        # Повторно вызываем prompt, который покажет список рамок
    await callback_select_frame_prompt(query, state) 

 
async def callback_back_to_titles_list(query: CallbackQuery, state: FSMContext):
    await query.answer()
        # Повторно вызываем prompt, который покажет список титулов
    await callback_select_title_prompt(query, state)
  
async def callback_select_title(query: CallbackQuery, state: FSMContext):
    await query.answer()
    title_id = query.data.split('_')[2]

    with Session() as session:
        player = ensure_player_profile_exists(session, query.from_user.id, query.from_user.username, query.from_user.full_name)
        if not player:
            await query.message.edit_text(f"Ошибка: Не удалось загрузить ваш профиль. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
        
        if title_id not in CUSTOM_TITLES:
            await query.message.edit_text(f"Ошибка: Неизвестный титул. {FACTION_EMOJIS['missed']}", reply_markup=None)
            await state.clear()
            return
        
        title_data = CUSTOM_TITLES[title_id]
        unlocked_titles = json.loads(player.unlocked_titles)
        
        if title_id not in unlocked_titles:
            # Попытка купить титул
            price_dollars = title_data.get('price_dollars', 0)
            price_diamonds = title_data.get('price_diamonds', 0.0)

            can_afford = True
            if price_dollars > 0 and player.dollars < price_dollars and player.user_id != BOT_OWNER_ID:
                can_afford = False
            if price_diamonds > 0 and player.diamonds < price_diamonds and player.user_id != BOT_OWNER_ID:
                can_afford = False

            if not can_afford:
                await query.message.edit_text(
                    f"У вас недостаточно средств, чтобы купить титул <b>{title_data['name_ru']}</b>. "
                    f"Требуется: {price_dollars}{FACTION_EMOJIS['dollars']} и {price_diamonds:.1f}{FACTION_EMOJIS['diamonds']}.\n"
                    f"Ваш баланс: {player.dollars}{FACTION_EMOJIS['dollars']} и {player.diamonds:.2f}{FACTION_EMOJIS['diamonds']}.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                await state.clear()
                return

            # Списываем средства
            if player.user_id != BOT_OWNER_ID:
                player.dollars -= price_dollars
                player.diamonds -= price_diamonds
            
            unlocked_titles.append(title_id)
            player.unlocked_titles = json.dumps(unlocked_titles)
            session.add(player)
            session.commit()
            
            current_dollars_display = "?" if player.user_id == BOT_OWNER_ID else str(player.dollars)
            current_diamonds_display = "?" if player.user_id == BOT_OWNER_ID else f"{player.diamonds:.2f}"
            await query.message.edit_text(
                f"Вы купили и выбрали титул <b>{title_data['name_ru']}</b>! {RESULT_EMOJIS['success']}\n"
                f"Текущий баланс: {current_dollars_display}{FACTION_EMOJIS['dollars']}, {current_diamonds_display}{FACTION_EMOJIS['diamonds']}.",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
            logging.info(f"Player {player.full_name} ({player.user_id}) bought and selected title '{title_id}'.")
            await state.clear()
            return

        # Если титул уже разблокирован, просто выбираем его
        player.selected_title = title_id
        session.add(player)
        session.commit()
        
        await query.message.edit_text(
            f"Вы выбрали титул <b>{title_data['name_ru']}</b>. {RESULT_EMOJIS['success']}\n"
            f"Проверьте свой профиль с помощью /profile.",
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
        logging.info(f"Player {player.full_name} ({player.user_id}) selected title '{title_id}'.")
        await state.clear()
 
async def process_farewell_message(message: Message, state: FSMContext):
    with Session() as session:
        try:
            user_id = message.from_user.id
            data = await state.get_data()
            game_id = data.get('game_id')
            player_full_name = data.get('player_full_name')
            player_role = data.get('player_role')

            game = session.get(Game, game_id)
            if not game or game.status == 'finished' or game.status == 'cancelled':
                await message.reply(f"Игра, в которой вы погибли, уже завершена или отменена. Ваше прощальное сообщение не будет отправлено. {PHASE_EMOJIS['death']}")
                await state.clear()
                return

            player_obj = session.query(Player).filter_by(user_id=user_id, game_id=game_id).first()
            if not player_obj or player_obj.is_alive:
                await message.reply(
                    f"Что-то пошло не так. Вы не мертвы в этой игре или игра не найдена. Ваше сообщение не отправлено. {PHASE_EMOJIS['death']}")
                await state.clear()
                return

            role_emoji = ROLE_EMOJIS.get(player_role, "?")
            player_role_ru = ROLE_NAMES_RU.get(player_role, player_role.capitalize())

            farewell_text = (
                f"{PHASE_EMOJIS['death']} Из жителей <a href='tg://user?id={user_id}'><b>{player_full_name}</b></a> "
                f"({role_emoji} {player_role_ru}), перед смертью услышали его крики:\n\n"
                f"<i>{message.text}</i>"
            )
            await bot.send_message(chat_id=game.chat_id, text=farewell_text, parse_mode=ParseMode.HTML)
            await message.reply(f"Ваше прощальное сообщение отправлено в чат игры {PHASE_EMOJIS['death']}")
            logging.info(f"Player {player_full_name} ({user_id}) sent farewell message to game {game_id}.")
            await state.clear()

        except Exception as e:
            logging.error(f"Ошибка при обработке прощального сообщения от {message.from_user.id}: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при отправке вашего прощального сообщения. {PHASE_EMOJIS['death']}")
            await state.clear()
            session.rollback()


 
async def handle_faction_message(message: Message, state: FSMContext):
    with Session() as session:
        try:
            user_id = message.from_user.id
            data = await state.get_data()
            game_id = data.get('game_id')
            player_full_name = data.get('player_full_name')
            player_role = data.get('player_role')

            game = session.get(Game, game_id)
            player = session.query(Player).filter_by(user_id=user_id, game_id=game_id, is_alive=True).first()
            if not game or not player or game.status != 'playing' or game.phase != 'night' or player.role not in ['mafia', 'don']:
                await message.reply("Вы не можете отправлять сообщения в чат фракции сейчас (игра не активна, не ночь, или вы не в мафии/мертвы).")
                await state.clear()
                return

            allies = session.query(Player).filter(
                Player.game_id == game.id,
                Player.is_alive == True,
                Player.role.in_(['mafia', 'don']),
                Player.user_id != user_id
            ).all()

            role_emoji = ROLE_EMOJIS.get(player.role, "?")
            chat_message = f"{role_emoji} <b>{player_full_name}</b>: {message.text}"

            for ally in allies:
                try:
                    await bot.send_message(chat_id=ally.user_id, text=chat_message, parse_mode=ParseMode.HTML)
                except TelegramForbiddenError:
                    logging.warning(f"Бот заблокирован союзником {ally.full_name}: не удалось отправить сообщение фракции.")
                except Exception as e:
                    logging.error(f"Ошибка при отправке сообщения фракции союзнику {ally.full_name}: {e}", exc_info=True)
            
            await state.set_state(GameState.waiting_for_faction_message)

        except Exception as e:
            logging.error(f"Ошибка при обработке сообщения фракции от {user_id}: {e}", exc_info=True)
            await message.reply(f"Произошла ошибка при отправке сообщения. {FACTION_EMOJIS['missed']}")
            await state.clear()
            session.rollback()

 
async def delete_non_game_messages(message: Message):
    # 0. Сообщения от самого бота никогда не удаляем
    if bot_self_info and message.from_user.id == bot_self_info.id:
        return

    with Session() as session:
        try:
            # 1. Любые команды (начинающиеся с '/') всегда разрешены
            if message.text and message.text.startswith('/'):
                logging.info(f"INFO: Command '{message.text.split()[0]}' from {message.from_user.full_name} (ID: {message.from_user.id}) PASSED THROUGH delete handler (command allowed).")
                return # Разрешено

            # 2. Администраторы могут использовать "точку" для написания без удаления
            if message.text and message.text.startswith('.'):
                try:
                    chat_member = await bot.get_chat_member(message.chat.id, message.from_user.id)
                    if chat_member.status in ['administrator', 'creator']:
                        new_text = message.text[1:].strip()
                        if new_text:
                            await message.edit_text(
                                text=new_text,
                                parse_mode=ParseMode.HTML
                            )
                            logging.info(f"ACTION: Admin message from {message.from_user.full_name} (ID: {message.from_user.id}) EDITED (removed dot prefix).")
                        else:
                            await message.delete()
                            logging.info(f"ACTION: Admin message from {message.from_user.full_name} (ID: {message.from_user.id}) DELETED (empty after dot prefix).")
                        return
                except TelegramBadRequest as e:
                    logging.warning(f"Failed to edit admin message {message.message_id} in chat {message.chat.id}: {e}")
                except TelegramForbiddenError:
                    logging.warning(f"Bot lacks permissions to edit messages in chat {message.chat.id}. Cannot process admin dot message.")
                except Exception as e:
                    logging.error(f"Error checking admin or editing dot message in chat {message.chat.id}: {e}", exc_info=True)


            session.expire_all()
            logging.debug(f"DEBUG: Session cache expired for chat {message.chat.id}.")

            game = session.query(Game).filter(Game.chat_id == message.chat.id).first()

            if not game:
                logging.info(f"INFO: No game found for chat {message.chat.id}. Message from {message.from_user.full_name} (ID: {message.from_user.id}) PASSED THROUGH delete handler (no game).")
                return

            logging.debug(f"DEBUG: In delete_non_game_messages, game {game.id} status is '{game.status}' and phase is '{game.phase}' for chat {message.chat.id}. ") 
            if game.status == 'finished' or game.status == 'cancelled':
                logging.info(f"INFO: Game {game.id} is {game.status}. Message from {message.from_user.full_name} (ID: {message.from_user.id}) PASSED THROUGH delete handler (game finished/cancelled).")
                return
            
            if game.status == 'waiting':
                logging.info(f"INFO: Game {game.id} is waiting. Message from {message.from_user.full_name} (ID: {message.from_user.id}) PASSED THROUGH delete handler (waiting phase).")
                return

            if game.status == 'playing':
                player = session.query(Player).filter_by(user_id=message.from_user.id, game_id=game.id).first()

                if not player or not player.is_alive:
                    await message.delete()
                    logging.info(f"ACTION: Deleted message from non-player/dead player {message.from_user.full_name} (ID: {message.from_user.id}) in game {game.id} (status: {game.status}, phase: {game.phase}).")
                    return

                if game.phase in ['day', 'voting', 'lynch_vote']:
                    logging.info(f"INFO: Game {game.id} is in {game.phase} phase. Message from alive player {message.from_user.full_name} (ID: {message.from_user.id}) PASSED THROUGH delete handler (allowed in current phase).")
                    return

                if game.phase == 'night':
                    await message.delete()
                    logging.info(f"ACTION: Deleted message from alive player {message.from_user.full_name} (ID: {message.from_user.id}) in game {game.id} (status: {game.status}, phase: {game.phase}) during NIGHT phase (not a command).")
                    return
            
            await message.delete()
            logging.warning(f"ACTION: Fallback delete of message from {message.from_user.full_name} (ID: {message.from_user.id}) in chat {message.chat.id}. This indicates an unhandled game state. Game status: {game.status}, phase: {game.phase}.")

        except TelegramBadRequest as e:
            logging.warning(f"Failed to delete message (ID: {message.message_id}) in chat {message.chat.id}: {e}")
        except TelegramForbiddenError:
            logging.error(f"Bot lacks permissions to delete messages in chat {message.chat.id}. Please grant 'Delete Messages' permission.")
        except Exception as e:
            logging.error(f"CRITICAL ERROR in delete_non_game_messages for chat {message.chat.id} from {message.from_user.full_name} (ID: {message.from_user.id}): {e}", exc_info=True)
            
async def main():
    global bot_self_info, BOT_ID, bot, dp
    logging.info("--- Main function started ---")

    connector = None
    if PROXY_URL:
        connector = aiohttp.ProxyConnector.from_url(PROXY_URL)
        logging.info(f"Using proxy: {PROXY_URL}")
    else:
        logging.info("Not using proxy.")

    aiogram_session_instance = aiohttp.ClientSession(connector=connector)

    async def custom_session_callable(bot_instance, method, timeout=None):
        if not isinstance(method, TelegramMethod):
             logging.error(f"custom_session_callable received unexpected method type: {type(method)}")
             raise TypeError(f"Expected TelegramMethod, got {type(method)}")

        base_api_url = "https://api.telegram.org"
        full_url = f"{base_api_url}/bot{BOT_TOKEN}/{method.__api_method__}"
        payload = method.model_dump_json()
        
        async with aiogram_session_instance.post(full_url, data=payload, headers={'Content-Type': 'application/json'}, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            resp.raise_for_status()
            json_response = await resp.json() # Получаем сырой JSON-ответ
            
            # ИЗМЕНЕНИЕ ЗДЕСЬ: Десериализуем JSON-ответ в ожидаемую Pydantic-модель
            # method.__returning__ - это тип, который aiogram ожидает в ответ
            # Например, для GetMe, method.__returning__ будет aiogram.types.user.User
            # json_response['result'] - это данные, которые нужно парсить
            if json_response.get('ok') and 'result' in json_response:
                # Используем Pydantic model_validate для парсинга
                # method.__returning__ - это Pydantic-модель (например, User)
                return method.__returning__.model_validate(json_response['result']) # <--- ИЗМЕНЕНИЕ
            else:
                raise Exception(f"Telegram API error: {json_response.get('description', 'Unknown error')}")

    bot = Bot(token=BOT_TOKEN,
              default=DefaultBotProperties(parse_mode=ParseMode.HTML),
              session=custom_session_callable,
              request_timeout=60.0)

    dp = Dispatcher(storage=MemoryStorage())

    print("Bot and Dispatcher initialized.")
    try:
        bot_self_info = await bot.get_me() # Теперь это должен быть объект User
        BOT_ID = bot_self_info.id # <--- Это должно сработать
        logging.info(f"Bot username: @{bot_self_info.username}, Bot ID: {BOT_ID}")
        print(f"Got bot info: @{bot_self_info.username}")
    except Exception as e:
        logging.error(f"Failed to get bot info: {e}", exc_info=True)
        print(f"ERROR: Failed to get bot info: {e}")
        await aiogram_session_instance.close()
        return

    init_db()
    logging.info("Database initialized.")
    print("Database initialized.")

     

    # Регистрация всех хэндлеров (они будут видеть глобальные bot и dp)
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_new_game, Command("new_game"))
    dp.message.register(cmd_join, Command("join"))
    dp.message.register(cmd_leave, Command("leave"))
    dp.message.register(cmd_start_game, Command("start_game"))
    dp.message.register(cmd_cancel_game, Command("cancel_game"))
    dp.message.register(cmd_players, Command("players"))
    dp.message.register(cmd_profile, Command("profile"))
    dp.message.register(cmd_donate, Command("donate"))
    dp.message.register(cmd_give_dollars, Command("give_dollars"))
    dp.message.register(cmd_give_diamonds, Command("give_diamonds"))

    dp.callback_query.register(callback_join_game, F.data.startswith('join_game_'))
    dp.callback_query.register(callback_start_game, F.data.startswith('start_game_'))
    dp.callback_query.register(callback_set_gender_prompt, F.data == "set_gender_prompt")
    dp.callback_query.register(callback_set_gender, F.data.startswith('set_gender_'))
    dp.callback_query.register(callback_vote, F.data.startswith('vote_'))
    dp.callback_query.register(callback_lynch_vote, F.data.startswith('lynch_'))
    dp.callback_query.register(callback_mafia_kill, F.data.startswith('mafia_kill_'))
    dp.callback_query.register(callback_doctor_heal, F.data.startswith('doctor_heal_'))
    dp.callback_query.register(callback_commissioner_check, F.data.startswith('com_check_'))
    dp.callback_query.register(callback_maniac_kill, F.data.startswith('maniac_kill_'))

    dp.message.register(process_farewell_message, GameState.waiting_for_farewell_message, F.text)
    dp.message.register(handle_faction_message, GameState.waiting_for_faction_message, F.text)

    dp.message.register(handle_gif, F.animation, F.chat.type == ChatType.PRIVATE)

    dp.message.register(delete_non_game_messages, F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))

    # Новые FSM хэндлеры для пожертвований
    dp.callback_query.register(callback_select_donate_group, GameState.waiting_for_donate_group_selection, F.data.startswith('select_donate_group_'))
    dp.callback_query.register(callback_donate_currency_selection, GameState.waiting_for_donate_currency_selection, F.data.startswith('donate_currency_'))
    dp.message.register(process_donate_dollars_amount, GameState.waiting_for_donate_dollars_amount, F.text)
    dp.message.register(process_donate_diamonds_amount, GameState.waiting_for_donate_diamonds_amount, F.text)

    # НОВЫЕ РЕГИСТРАЦИИ ДЛЯ КАСТОМИЗАЦИИ
    dp.callback_query.register(callback_select_frame_prompt, F.data == "select_frame_prompt")
    dp.callback_query.register(callback_preview_frame, GameState.waiting_for_frame_selection, F.data.startswith('preview_frame_'))
    dp.callback_query.register(callback_confirm_frame_action, GameState.waiting_for_frame_preview_action, F.data.startswith('confirm_frame_action_'))
    dp.callback_query.register(callback_back_to_frames_list, F.data == "back_to_frames_list")
    dp.callback_query.register(callback_back_to_profile, F.data == "back_to_profile")
    dp.callback_query.register(callback_select_title_prompt, F.data == "select_title_prompt")
    dp.callback_query.register(callback_preview_title, GameState.waiting_for_title_selection, F.data.startswith('preview_title_'))
    dp.callback_query.register(callback_confirm_title_action, GameState.waiting_for_title_preview_action, F.data.startswith('confirm_title_action_'))
    dp.callback_query.register(callback_back_to_titles_list, F.data == "back_to_titles_list")

    # Исправлена логика для callback_donate_prompt
    dp.callback_query.register(callback_donate_prompt_handler, F.data == "donate_prompt")
    print("All handlers registered.")
    
    # Объявление callback_donate_prompt_handler должно быть выше его регистрации
    async def callback_donate_prompt_handler(query: CallbackQuery):
        await query.answer()

        async def edit_callback_message_func(text, reply_markup=None, parse_mode=ParseMode.HTML):
            try:
                await query.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            except TelegramBadRequest as e:
                logging.warning(f"Не удалось отредактировать сообщение в callback_donate_prompt для пользователя {query.from_user.id}: {e}")
                await bot.send_message(query.from_user.id, text, reply_markup=reply_markup, parse_mode=parse_mode)
            except Exception as e:
                logging.error(f"Ошибка при редактировании сообщения в callback_donate_prompt для пользователя {query.from_user.id}: {e}", exc_info=True)
                await bot.send_message(query.from_user.id, text, reply_markup=reply_markup, parse_mode=parse_mode)

        state = dp.fsm.get_context(bot=bot, chat_id=query.from_user.id, user_id=query.from_user.id)
        await _process_donate_command_logic(
            query.from_user.id,
            query.from_user.username,
            query.from_user.full_name,
            query.message.chat.id,
            state,
            edit_callback_message_func
        )
        
    scheduler_needs_restart = False
    with Session() as session:
        active_games_in_db = session.query(Game).filter(Game.status == 'playing').all()
        for game in active_games_in_db:
            if game.phase_end_time and game.phase_end_time > datetime.datetime.now():
                logging.info(f"Rescheduling active game {game.id} job. Current phase: {game.phase}, next run at: {game.phase_end_time}")
                job_id = f"end_{game.phase}_game_{game.id}" if game.phase != 'night' else f"end_night_processing_game_{game.id}"
                if game.phase == 'day':
                    scheduler.add_job(end_day_phase, 'date', run_date=game.phase_end_time, args=[game.id], id=job_id)
                elif game.phase == 'voting':
                    scheduler.add_job(end_voting_phase, 'date', run_date=game.phase_end_time, args=[game.id], id=job_id)
                elif game.phase == 'lynch_vote':
                    scheduler.add_job(end_lynch_voting_phase, 'date', run_date=game.phase_end_time, args=[game.id], id=job_id)
                elif game.phase == 'night':
                    scheduler.add_job(end_night_phase_processing, 'date', run_date=game.phase_end_time, args=[game.id], id=job_id)
            else:
                logging.warning(f"Game {game.id} found with expired phase {game.phase} end time. Forcing phase transition.")
                if game.phase == 'day':
                    await end_day_phase(game.id)
                elif game.phase == 'voting':
                    await end_voting_phase(game.id)
                elif game.phase == 'lynch_vote':
                    await end_lynch_voting_phase(game.id)
                elif game.phase == 'night':
                    await end_night_phase_processing(game.id)
    scheduler.start()
    logging.info("Scheduler started.")
    print("Scheduler started.")
    
    # Теперь вызываем polling один раз, и это последняя асинхронная операция в main
    try:
        logging.info("Starting bot polling")
        await dp.start_polling(bot)
    finally:
        logging.info("Bot polling stopped. Closing session.")
        await aiogram_session_instance.close()
        logging.info("Session closed. Application exit.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

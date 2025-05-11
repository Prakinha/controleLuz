use std::{env, sync::Arc, io::{self, Write}};
use log::{info, LevelFilter};
use tapo::{ApiClient, ColorLightHandler};
use tokio::{
    sync::{Mutex, mpsc},
    time::Duration,
};
use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use futures::{SinkExt, StreamExt};
use futures::TryFutureExt;
use serde::Deserialize; // Se necessário, caso esteja utilizando em outra parte do código
use futures::future::join_all;
use std::convert::TryInto;

struct Device {
    ip: String,
    handler: ColorLightHandler,
    brightness: u8, // Luminosidade atual (0-100)
}

struct AppState {
    devices: Vec<Device>,
    color: (u8, u8, u8), // (R, G, B) em 0..255
}

#[derive(Debug)]
enum Command {
    Switch,
    White,
    Red,
    Green,
    IncreaseBrightness,
    DecreaseBrightness,
    ResetBrightness,
    // Manipulação RGB individual:
    IncreaseRed,
    DecreaseRed,
    IncreaseGreen,
    DecreaseGreen,
    IncreaseBlue,
    DecreaseBlue,
    // Transição suave partindo da cor atual (F10)
    Transition,
    Exit,
}

// -------------------------------------------------------
// 1) Conversão de RGB -> Hue/Sat/Val no padrão Tapo
fn rgb_to_hsv_tapo(r: u8, g: u8, b: u8) -> (u16, u8, u8) {
    let rf = r as f32 / 255.0;
    let gf = g as f32 / 255.0;
    let bf = b as f32 / 255.0;

    let max_c = rf.max(gf).max(bf);
    let min_c = rf.min(gf).min(bf);
    let delta = max_c - min_c;

    // Hue
    let mut hue = if delta == 0.0 {
        0.0
    } else if (max_c - rf).abs() < f32::EPSILON {
        60.0 * (((gf - bf) / delta) % 6.0)
    } else if (max_c - gf).abs() < f32::EPSILON {
        60.0 * (((bf - rf) / delta) + 2.0)
    } else {
        60.0 * (((rf - gf) / delta) + 4.0)
    };
    if hue < 0.0 {
        hue += 360.0;
    }

    // Saturation
    let sat = if max_c == 0.0 {
        0.0
    } else {
        delta / max_c
    };

    // Value
    let val = max_c;

    let hue_tapo = hue.round() as u16;         // 0..360
    let sat_tapo = (sat * 100.0).round() as u8; // 0..100
    let val_tapo = (val * 100.0).round() as u8; // 0..100

    (hue_tapo, sat_tapo, val_tapo)
}

// -------------------------------------------------------
// 2) Atualiza a cor em todos os dispositivos
async fn update_color_in_devices(state: Arc<Mutex<AppState>>) {
    let mut st = state.lock().await;
    let (r, g, b) = st.color;

    let (hue, sat, val) = rgb_to_hsv_tapo(r, g, b);
    info!(
        "Atualizando cor para RGB({},{},{}) => H={} S={} V={}",
        r, g, b, hue, sat, val
    );

    let futures = st.devices.iter().map(|device| async move {
        match device
            .handler
            .set()
            .hue_saturation(hue, sat)
            .brightness(val)
            .send(&device.handler)
            .await
        {
            Ok(_) => info!("Cor atualizada com sucesso no dispositivo {}", device.ip),
            Err(e) => eprintln!("Falha ao atualizar cor no dispositivo {}: {}", device.ip, e),
        }
    });
    join_all(futures).await;
}

// -------------------------------------------------------
// 3) Altera canal de cor (R,G,B) com passo de 10
async fn handle_color_channel_change(state: Arc<Mutex<AppState>>, channel: char, delta: i16) {
    let mut st = state.lock().await;
    let (ref mut r, ref mut g, ref mut b) = st.color;

    let step = 10; // passo = 10

    match channel {
        'r' => {
            if delta > 0 {
                *r = (*r as i16 + step).clamp(0, 255) as u8;
            } else {
                *r = (*r as i16 - step).clamp(0, 255) as u8;
            }
        }
        'g' => {
            if delta > 0 {
                *g = (*g as i16 + step).clamp(0, 255) as u8;
            } else {
                *g = (*g as i16 - step).clamp(0, 255) as u8;
            }
        }
        'b' => {
            if delta > 0 {
                *b = (*b as i16 + step).clamp(0, 255) as u8;
            } else {
                *b = (*b as i16 - step).clamp(0, 255) as u8;
            }
        }
        _ => {}
    }

    info!("Nova cor local: RGB({},{},{})", *r, *g, *b);
}

// -------------------------------------------------------
// 4) Função que faz a transição suave entre duas cores
async fn transition_colors(
    state: Arc<Mutex<AppState>>,
    color_start: (u8, u8, u8),
    color_end: (u8, u8, u8),
) {
    let steps = 50;
    let total_duration = Duration::from_secs(5); // 5s de transição
    let step_duration = total_duration / steps;

    for i in 0..=steps {
        let t = i as f32 / steps as f32; // fração 0..1

        let r = color_start.0 as f32 + t * ((color_end.0 as i32 - color_start.0 as i32) as f32);
        let g = color_start.1 as f32 + t * ((color_end.1 as i32 - color_start.1 as i32) as f32);
        let b = color_start.2 as f32 + t * ((color_end.2 as i32 - color_start.2 as i32) as f32);

        {
            // Atualiza o estado
            let mut st = state.lock().await;
            st.color = (r.round() as u8, g.round() as u8, b.round() as u8);
        }

        // Atualiza as lâmpadas
        update_color_in_devices(state.clone()).await;

        // Espera entre cada "frame" da transição
        tokio::time::sleep(step_duration).await;
    }
}

// Auxiliar para parsear "R,G,B"
fn parse_rgb(input: &str) -> Option<(u8, u8, u8)> {
    let parts: Vec<&str> = input.split(',').collect();
    if parts.len() != 3 {
        return None;
    }
    let r = parts[0].trim().parse().ok()?;
    let g = parts[1].trim().parse().ok()?;
    let b = parts[2].trim().parse().ok()?;
    Some((r, g, b))
}

// 5) Trata comando Transition: parte da cor atual e vai até a cor final digitada
async fn handle_transition(state: Arc<Mutex<AppState>>) -> Result<()> {
    // Precisamos sair do modo raw para ler do terminal
    disable_raw_mode()?;
    print!("Digite a cor final no formato R,G,B: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    // Volta ao modo raw
    enable_raw_mode()?;

    let color_final = match parse_rgb(&input) {
        Some(c) => c,
        None => {
            eprintln!("Erro ao parsear cor final!");
            return Ok(());
        }
    };

    // Cor inicial é a cor atual do estado
    let color_start = {
        let st = state.lock().await;
        st.color
    };

    info!(
        "Iniciando transição suave de RGB({},{},{}) para RGB({},{},{})...",
        color_start.0, color_start.1, color_start.2,
        color_final.0, color_final.1, color_final.2
    );

    // Executa a transição
    transition_colors(state.clone(), color_start, color_final).await;
    info!("Transição concluída!");

    Ok(())
}

// -------------------------------------------------------
// main e keyboard_listener - agora F10 dispara a transição
#[tokio::main]
async fn main() -> Result<()> {
    // Configuração de logging
    let log_level = env::var("RUST_LOG")
        .unwrap_or_else(|_| "info".to_string())
        .parse()
        .unwrap_or(LevelFilter::Info);

    pretty_env_logger::formatted_timed_builder()
        .filter(None, log_level)
        .init();

    println!("Logger inicializado.");
    info!("Logger configurado com nível: {:?}", log_level);

    // Credenciais
    let tapo_username = env::var("TAPO_USERNAME")?;
    let tapo_password = env::var("TAPO_PASSWORD")?;

    // Endereços IP
    let ip_addresses = vec![
        "192.168.1.79",
        "192.168.1.15",
        // "192.168.1.77",
        "192.168.1.115",
        // "192.168.1.114",
    ];

    // Inicializar dispositivos
    let devices_futures: Vec<_> = ip_addresses
        .iter()
        .map(|ip| {
            ApiClient::new(tapo_username.clone(), tapo_password.clone())
                .l530(ip.to_string())
                .map_ok(|handler| Device {
                    ip: ip.to_string(),
                    handler,
                    brightness: 100, // Inicializa com 100% de brilho
                })
        })
        .collect();

    let devices_result = join_all(devices_futures).await;
    let devices = match devices_result.into_iter().collect::<Result<Vec<_>, _>>() {
        Ok(devs) => {
            info!("Todos os dispositivos foram inicializados.");
            devs
        }
        Err(e) => {
            eprintln!("Erro ao inicializar dispositivos: {}", e);
            return Err(anyhow::anyhow!(e));
        }
    };

    // Nosso estado (cor inicial = branco)
    let state = Arc::new(Mutex::new(AppState {
        devices,
        color: (255, 255, 255),
    }));

    // Atualiza a cor inicial
    update_color_in_devices(state.clone()).await;

    // Canal de comandos
    let (tx, mut rx) = mpsc::channel::<Command>(32);

    // Task de keyboard
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = keyboard_listener(tx_clone).await {
            eprintln!("Erro no listener de teclado: {}", e);
        }
    });

    info!("Listener de teclado iniciado.");

    // Loop de comandos
    loop {
        tokio::select! {
            Some(command) = rx.recv() => {
                match command {
                    Command::Switch => {
                        handle_switch(state.clone()).await;
                    },
                    Command::White => {
                        handle_set_color(state.clone(), 2700, None).await;
                    },
                    Command::Red => {
                        handle_set_color(state.clone(), 0, Some(100)).await;
                    },
                    Command::Green => {
                        handle_set_color(state.clone(), 120, Some(100)).await;
                    },
                    Command::IncreaseBrightness => {
                        handle_change_brightness(state.clone(), 5).await;
                    },
                    Command::DecreaseBrightness => {
                        handle_change_brightness(state.clone(), u8::MAX - 5).await;
                    },
                    Command::ResetBrightness => {
                        handle_reset_brightness(state.clone()).await;
                    },
                    Command::IncreaseRed => {
                        handle_color_channel_change(state.clone(), 'r', 1).await;
                        update_color_in_devices(state.clone()).await;
                    },
                    Command::DecreaseRed => {
                        handle_color_channel_change(state.clone(), 'r', -1).await;
                        update_color_in_devices(state.clone()).await;
                    },
                    Command::IncreaseGreen => {
                        handle_color_channel_change(state.clone(), 'g', 1).await;
                        update_color_in_devices(state.clone()).await;
                    },
                    Command::DecreaseGreen => {
                        handle_color_channel_change(state.clone(), 'g', -1).await;
                        update_color_in_devices(state.clone()).await;
                    },
                    Command::IncreaseBlue => {
                        handle_color_channel_change(state.clone(), 'b', 1).await;
                        update_color_in_devices(state.clone()).await;
                    },
                    Command::DecreaseBlue => {
                        handle_color_channel_change(state.clone(), 'b', -1).await;
                        update_color_in_devices(state.clone()).await;
                    },
                    // Transição a partir do valor atual até a cor desejada
                    Command::Transition => {
                        if let Err(e) = handle_transition(state.clone()).await {
                            eprintln!("Erro ao processar transição: {}", e);
                        }
                    },
                    Command::Exit => {
                        info!("Comando de saída recebido. Encerrando...");
                        break;
                    },
                }
            },
            else => {}
        }
    }

    // Final
    disable_raw_mode()?;
    info!("Programa finalizado.");
    Ok(())
}

async fn keyboard_listener(tx: mpsc::Sender<Command>) -> Result<()> {
    enable_raw_mode()?;

    println!("Listener de teclado ativo. Use as seguintes teclas:");
    println!("F1: Alternar ligar/desligar");
    println!("F2: Definir cor para branco quente");
    println!("F3: Definir cor para vermelho");
    println!("F4: Definir cor para verde");
    println!("F24 (ex-9): Aumentar luminosidade");
    println!("F23 (ex-8): Diminuir luminosidade");
    println!("F22 (ex-7): Resetar luminosidade");
    println!("F10: Transição suave a partir da cor atual -> cor final");
    println!("Home: Sair");
    println!("--- RGB ---");
    println!("W/S: + Vermelho / - Vermelho (passo = 10)");
    println!("E/D: + Verde / - Verde (passo = 10)");
    println!("R/F: + Azul / - Azul (passo = 10)");

    loop {
        if event::poll(Duration::from_millis(300))? {
            if let Event::Key(key_event) = event::read()? {
                match key_event.code {
                    KeyCode::F(n) if n == 13 => tx.send(Command::Switch).await?,
                    KeyCode::F(n) if n == 14 => tx.send(Command::White).await?,
                    KeyCode::F(n) if n == 15 => tx.send(Command::Red).await?,
                    KeyCode::F(n) if n == 18 => tx.send(Command::Green).await?,
                    KeyCode::F(n) if n == 24 => tx.send(Command::IncreaseBrightness).await?,
                    KeyCode::F(n) if n == 23 => tx.send(Command::DecreaseBrightness).await?,
                    KeyCode::F(n) if n == 22 => tx.send(Command::ResetBrightness).await?,

                    // Agora definimos F10 (n=10) para a transição
                    KeyCode::F(n) if n == 10 => tx.send(Command::Transition).await?,

                    KeyCode::Home => {
                        tx.send(Command::Exit).await?;
                        break;
                    },

                    // RGB
                    KeyCode::Char('w') => tx.send(Command::IncreaseRed).await?,
                    KeyCode::Char('s') => tx.send(Command::DecreaseRed).await?,
                    KeyCode::Char('e') => tx.send(Command::IncreaseGreen).await?,
                    KeyCode::Char('d') => tx.send(Command::DecreaseGreen).await?,
                    KeyCode::Char('r') => tx.send(Command::IncreaseBlue).await?,
                    KeyCode::Char('f') => tx.send(Command::DecreaseBlue).await?,

                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    Ok(())
}

// -------------------------------------------------------
// Restante das funções (handle_switch, handle_set_color, etc.)
async fn handle_switch(state: Arc<Mutex<AppState>>) {
    info!("Comando: Alternar estado (ligar/desligar).");
    let st = state.lock().await;
    let futures = st.devices.iter().map(|device| async move {
        match device.handler.get_device_info().await {
            Ok(info_dev) => {
                if info_dev.device_on {
                    info!("Desligando {}...", device.ip);
                    match device.handler.off().await {
                        Ok(_) => info!("{} desligado!", device.ip),
                        Err(e) => eprintln!("Erro ao desligar {}: {}", device.ip, e),
                    }
                } else {
                    info!("Ligando {}...", device.ip);
                    match device.handler.on().await {
                        Ok(_) => info!("{} ligado!", device.ip),
                        Err(e) => eprintln!("Erro ao ligar {}: {}", device.ip, e),
                    }
                }
            }
            Err(e) => eprintln!("Erro ao obter info de {}: {}", device.ip, e),
        }
    });
    join_all(futures).await;
}

async fn handle_set_color(state: Arc<Mutex<AppState>>, color_temp: u32, saturation: Option<u8>) {
    if let Some(sat) = saturation {
        info!("Definindo cor Hue={} Saturation={}%", color_temp, sat);
    } else {
        info!("Definindo temperatura de cor = {}K", color_temp);
    }
    let st = state.lock().await;
    let futures = st.devices.iter().map(|device| async move {
        if let Some(sat_val) = saturation {
            if let Err(e) = device.handler
                .set()
                .hue_saturation(color_temp as u16, sat_val)
                .send(&device.handler).await
            {
                eprintln!("Erro ao setar Hue/Sat em {}: {}", device.ip, e);
            }
        } else {
            if let Err(e) = device.handler
                .set()
                .color_temperature(color_temp as u16)
                .send(&device.handler).await
            {
                eprintln!("Erro ao setar ColorTemp em {}: {}", device.ip, e);
            }
        }
    });
    join_all(futures).await;
}

async fn handle_change_brightness(state: Arc<Mutex<AppState>>, delta: u8) {
    let mut st = state.lock().await;
    let futures = st.devices.iter_mut().map(|device| async {
        let new_brightness = if delta != u8::MAX - 5 {
            (device.brightness + delta).min(100)
        } else {
            device.brightness.saturating_sub(5)
        };
        if delta != u8::MAX - 5 && device.brightness < 100 {
            device.brightness = new_brightness;
            info!("Aumentando brilho -> {}%", device.brightness);
        } else if delta == u8::MAX - 5 && device.brightness > 0 {
            device.brightness = new_brightness;
            info!("Diminuindo brilho -> {}%", device.brightness);
        } else {
            return;
        }
        if let Err(e) = device.handler
            .set()
            .brightness(device.brightness)
            .send(&device.handler).await
        {
            eprintln!("Falha ao ajustar brilho em {}: {}", device.ip, e);
        }
    });
    join_all(futures).await;
}

async fn handle_reset_brightness(state: Arc<Mutex<AppState>>) {
    let mut st = state.lock().await;
    let futures = st.devices.iter_mut().map(|device| async {
        device.brightness = 100;
        info!("Reset brilho -> 100% em {}", device.ip);
        if let Err(e) = device.handler
            .set()
            .brightness(100)
            .send(&device.handler).await
        {
            eprintln!("Falha ao resetar brilho em {}: {}", device.ip, e);
        }
    });
    join_all(futures).await;
}

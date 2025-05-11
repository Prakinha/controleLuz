use std::{env, sync::Arc};
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
use std::convert::TryInto;
use serde::Deserialize; // Se necessário, caso esteja utilizando em outra parte do código
use futures::future::join_all;

// Estrutura para armazenar informações do dispositivo
struct Device {
    ip: String,
    handler: ColorLightHandler,
    brightness: u8, // Luminosidade atual (0-100)
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
    Exit,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Configuração de logging
    let log_level = env::var("RUST_LOG")
        .unwrap_or_else(|_| "info".to_string())
        .parse()
        .unwrap_or(LevelFilter::Info);

    pretty_env_logger::formatted_timed_builder()
        .filter(None, log_level) // Captura todos os logs
        .init();

    println!("Logger inicializado."); // Confirmar inicialização
    info!("Logger configurado com nível: {:?}", log_level); // Verificar configuração

    // Recuperar credenciais e IPs dos dispositivos a partir de variáveis de ambiente
    let tapo_username = env::var("TAPO_USERNAME")?;
    let tapo_password = env::var("TAPO_PASSWORD")?;

    info!("Credenciais recuperadas.");
    info!("IPs dos dispositivos: {:?}", [
    //     "192.168.7.50",
    //    // "192.168.7.49",
    //     "192.168.7.47",


    //ips techmit
    "192.168.1.79",
    "192.168.1.15",
    "192.168.1.77",
    "192.168.1.115",
    "192.168.1.114",
    ]);

    let ip_addresses = vec![
    //     "192.168.7.50",
    //    // "192.168.7.49",
    //     "192.168.7.47",

        //ips techmit
    "192.168.1.79",
    "192.168.1.15",
    "192.168.1.77",
    "192.168.1.115",
    "192.168.1.114",
    ];

    // Inicializar dispositivos
    let devices_futures: Vec<_> = ip_addresses
        .iter()
        .map(|ip| {
            ApiClient::new(tapo_username.clone(), tapo_password.clone())
                .l530(ip.to_string()) // Use o método correto para seu dispositivo
                .map_ok(|handler| Device {
                    ip: ip.to_string(),
                    handler,
                    brightness: 100, // Inicializa com 100% de brilho
                })
        })
        .collect();

    // Tentar inicializar todos os dispositivos
    let devices_result = join_all(devices_futures).await;

    let devices = match devices_result.into_iter().collect::<Result<Vec<_>, _>>() {
        Ok(devices) => {
            info!("Todos os dispositivos foram inicializados.");
            devices
        }
        Err(e) => {
            eprintln!("Erro ao inicializar dispositivos: {}", e);
            return Err(anyhow::anyhow!(e) as anyhow::Error);
        }
    };

    // Usar Arc e Mutex para compartilhar dispositivos entre tarefas
    let devices = Arc::new(Mutex::new(devices));

    // Criar um canal para comunicação entre o listener de teclado e o controlador de dispositivos
    let (tx, mut rx) = mpsc::channel::<Command>(32);

    // Clonar para a tarefa do listener de teclado
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        if let Err(e) = keyboard_listener(tx_clone).await {
            eprintln!("Erro no listener de teclado: {}", e);
        }
    });

    info!("Listener de teclado iniciado.");

    // Loop principal para processar comandos
    loop {
        tokio::select! {
            Some(command) = rx.recv() => {
                match command {
                    Command::Switch => {
                        handle_switch(devices.clone()).await;
                    },
                    Command::White => {
                        handle_set_color(devices.clone(), 2700, None).await;
                    },
                    Command::Red => {
                        handle_set_color(devices.clone(), 0, Some(100)).await;
                    },
                    Command::Green => {
                        handle_set_color(devices.clone(), 120, Some(100)).await;
                    },
                    Command::IncreaseBrightness => {
                        handle_change_brightness(devices.clone(), 5).await;
                    },
                    Command::DecreaseBrightness => {
                        handle_change_brightness(devices.clone(), u8::MAX - 5).await;
                    },
                    Command::ResetBrightness => {
                        handle_reset_brightness(devices.clone()).await;
                    },
                    Command::Exit => {
                        info!("Comando de saída recebido. Encerrando...");
                        break;
                    },
                }
            },
            else => {
                // Nenhum comando recebido, continuar
            }
        }
    }

    // Finalizar o modo raw do terminal
    disable_raw_mode()?;
    info!("Programa finalizado.");
    Ok(())
}

async fn keyboard_listener(tx: mpsc::Sender<Command>) -> Result<()> {
    // Habilitar modo raw para capturar teclas sem esperar por Enter
    enable_raw_mode()?;

    println!("Listener de teclado ativo. Use as seguintes teclas:");
    println!("F1: Alternar ligar/desligar");
    println!("F2: Definir cor para branco quente");
    println!("F3: Definir cor para vermelho");
    println!("F4: Definir cor para verde");
    println!("9: Aumentar luminosidade");
    println!("8: Diminuir luminosidade");
    println!("7: Resetar luminosidade para 100%");
    println!("ESC: Sair");

    loop {
        // Esperar por um evento de teclado
        if event::poll(Duration::from_secs(1))? {
            if let Event::Key(key_event) = event::read()? {
                match key_event.code {
                    KeyCode::F(n) if n == 13 => {
                        tx.send(Command::Switch).await?;
                    }
                    KeyCode::F(n) if n == 14 => {
                        tx.send(Command::White).await?;
                    }
                    KeyCode::F(n) if n == 15 => {
                        tx.send(Command::Red).await?;
                    }
                    KeyCode::F(n) if n == 18 => {
                        tx.send(Command::Green).await?;
                    }
                    //KeyCode::Char('9') => {
                    KeyCode::F(n) if n == 24 => {
                        tx.send(Command::IncreaseBrightness).await?;
                    }
                    //KeyCode::Char('8') => {
                    KeyCode::F(n) if n == 23 => {
                        tx.send(Command::DecreaseBrightness).await?;
                    }
                   // KeyCode::Char('7') => {
                   KeyCode::F(n) if n == 22 => {
                        tx.send(Command::ResetBrightness).await?;
                    }
                    KeyCode::Home => {
                        tx.send(Command::Exit).await?;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    // Desabilitar modo raw ao sair
    disable_raw_mode()?;
    Ok(())
}

async fn handle_switch(devices: Arc<Mutex<Vec<Device>>>) {
    info!("Comando: Alternar estado dos dispositivos (ligar/desligar).");
    let devices = devices.clone();
    let devices = devices.lock().await;
    let futures = devices.iter().map(|device| async {
        info!("Obtendo estado atual do dispositivo {}...", device.ip);
        match device.handler.get_device_info().await {
            Ok(device_info) => {
                match device_info.device_on {
                    true => {
                        info!("Device {} está ligado. Desligando...", device.ip);
                        match device.handler.off().await {
                            Ok(_) => {
                                info!("Dispositivo {} desligado com sucesso.", device.ip);
                            }
                            Err(e) => {
                                eprintln!("Falha ao desligar dispositivo {}: {}", device.ip, e);
                                info!("Detalhes do erro ao desligar dispositivo {}: {:?}", device.ip, e);
                            }
                        }
                    }
                    false => {
                        info!("Device {} está desligado. Ligando...", device.ip);
                        match device.handler.on().await {
                            Ok(_) => {
                                info!("Dispositivo {} ligado com sucesso.", device.ip);
                            }
                            Err(e) => {
                                eprintln!("Falha ao ligar dispositivo {}: {}", device.ip, e);
                                info!("Detalhes do erro ao ligar dispositivo {}: {:?}", device.ip, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Erro ao obter informações do dispositivo {}: {}", device.ip, e);
                info!("Detalhes do erro ao obter informações do dispositivo {}: {:?}", device.ip, e);
            }
        }
    });
    join_all(futures).await;
}

async fn handle_set_color(devices: Arc<Mutex<Vec<Device>>>, color_temp: u32, saturation: Option<u8>) {
    if let Some(sat) = saturation {
        info!("Comando: Definir cor com Hue {} e Saturação {} em todos os dispositivos.", color_temp, sat);
    } else {
        info!("Comando: Definir cor com Temperatura de Cor {}K em todos os dispositivos.", color_temp);
    }
    let devices = devices.clone();
    let devices = devices.lock().await;
    let futures = devices.iter().map(|device| async {
        if let Some(sat_val) = saturation {
            // Definir hue e saturação
            info!("Definindo cor para Hue {} e Saturação {} no dispositivo {}...", color_temp, sat_val, device.ip);
            match device.handler.set().hue_saturation(color_temp.try_into().unwrap(), sat_val).send(&device.handler).await {
                Ok(_) => info!("Dispositivo {} definido para Hue {} e Saturação {} com sucesso.", device.ip, color_temp, sat_val),
                Err(e) => {
                    eprintln!("Falha ao definir cor no dispositivo {}: {}", device.ip, e);
                    info!("Detalhes do erro ao definir cor no dispositivo {}: {:?}", device.ip, e);
                },
            }
        } else {
            // Definir temperatura de cor
            info!("Definindo temperatura de cor para {}K no dispositivo {}...", color_temp, device.ip);
            match device.handler.set().color_temperature(color_temp.try_into().unwrap()).send(&device.handler).await {
                Ok(_) => info!("Dispositivo {} definido para temperatura de cor {}K com sucesso.", device.ip, color_temp),
                Err(e) => {
                    eprintln!("Falha ao definir temperatura de cor no dispositivo {}: {}", device.ip, e);
                    info!("Detalhes do erro ao definir temperatura de cor no dispositivo {}: {:?}", device.ip, e);
                },
            }
        }
    });
    join_all(futures).await;
}

async fn handle_change_brightness(devices: Arc<Mutex<Vec<Device>>>, delta: u8) {
    info!("Comando: Alterar luminosidade em {}% em todos os dispositivos.", if delta != u8::MAX - 5 { 5 } else { -5 });
    let devices = devices.clone();
    let mut devices = devices.lock().await;
    let futures = devices.iter_mut().map(|device| async {
        let new_brightness = if delta != u8::MAX - 5 {
            (device.brightness + delta).min(100)
        } else {
            device.brightness.saturating_sub(5)
        };
        if delta != u8::MAX - 5 && device.brightness < 100 {
            device.brightness = new_brightness;
            info!("Aumentando luminosidade para {}% no dispositivo {}...", device.brightness, device.ip);
        } else if delta == u8::MAX - 5 && device.brightness > 0 {
            device.brightness = new_brightness;
            info!("Diminuindo luminosidade para {}% no dispositivo {}...", device.brightness, device.ip);
        } else {
            if delta != u8::MAX - 5 {
                info!("Dispositivo {} já está com luminosidade máxima (100%).", device.ip);
            } else {
                info!("Dispositivo {} já está com luminosidade mínima (0%).", device.ip);
            }
            return;
        }
        match device.handler.set().brightness(device.brightness).send(&device.handler).await {
            Ok(_) => info!("Dispositivo {} luminosidade ajustada para {}%.", device.ip, device.brightness),
            Err(e) => {
                eprintln!("Falha ao ajustar luminosidade no dispositivo {}: {}", device.ip, e);
                info!("Detalhes do erro ao ajustar luminosidade no dispositivo {}: {:?}", device.ip, e);
            },
        }
    });
    join_all(futures).await;
}

async fn handle_reset_brightness(devices: Arc<Mutex<Vec<Device>>>) {
    info!("Comando: Resetar luminosidade para 100% em todos os dispositivos.");
    let devices = devices.clone();
    let mut devices = devices.lock().await;
    let futures = devices.iter_mut().map(|device| async {
        device.brightness = 100;
        info!("Resetando luminosidade para 100% no dispositivo {}...", device.ip);
        match device.handler.set().brightness(device.brightness).send(&device.handler).await {
            Ok(_) => info!("Dispositivo {} luminosidade resetada para 100%.", device.ip),
            Err(e) => {
                eprintln!("Falha ao resetar luminosidade no dispositivo {}: {}", device.ip, e);
                info!("Detalhes do erro ao resetar luminosidade no dispositivo {}: {:?}", device.ip, e);
            },
        }
    });
    join_all(futures).await;
}

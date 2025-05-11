use std::{env, sync::Arc};
use log::{info, LevelFilter};
use serde::Deserialize;
use tapo::{ApiClient, ColorLightHandler};
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures::{SinkExt, StreamExt, TryFutureExt};
use anyhow::Result;
use url::Url;

// Estrutura para armazenar informações do dispositivo
struct Device {
    ip: String,
    handler: ColorLightHandler,
}

#[derive(Deserialize, Debug)]
struct IncomingMessage {
    #[serde(rename = "type")]
    msg_type: String,

    #[serde(rename = "content")]
    content: Option<String>,
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
        // "192.168.7.50",
        // "192.168.7.49",
        // "192.168.7.48",
        "192.168.7.47",
    ]);

    let ip_addresses = vec![
        // "192.168.7.50",
        // "192.168.7.49",
        // "192.168.7.48",
        "192.168.7.47",
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
                })
        })
        .collect();

    // Tentar inicializar todos os dispositivos
    let devices_result = futures::future::try_join_all(devices_futures).await;

    let devices = match devices_result {
        Ok(devices) => {
            info!("Todos os dispositivos foram inicializados.");
            devices
        }
        Err(e) => {
            eprintln!("Erro ao inicializar dispositivos: {}", e);
            return Err(anyhow::anyhow!(e));
        }
    };

    // // Teste manual para ligar uma lâmpada específica
    // if let Some(device) = devices.iter().find(|d| d.ip == "192.168.7.50") {
    //     info!("Testando controle manual da lâmpada {}", device.ip);
    //     match device.handler.on().await {
    //         Ok(_) => info!("Lâmpada {} ligada com sucesso (teste manual).", device.ip),
    //         Err(e) => eprintln!("Falha ao ligar lâmpada {} (teste manual): {}", device.ip, e),
    //     }
    // }

    // Usar Arc e Mutex para compartilhar dispositivos entre tarefas
    let devices = Arc::new(Mutex::new(devices));

    // Conectar ao servidor WebSocket
    let ws_url = Url::parse("ws://192.168.7.23:420")?;
    let (ws_stream, _) = connect_async(ws_url).await.expect("Falha ao conectar ao WebSocket");
    info!("Conectado ao WebSocket.");

    let (mut write, mut read) = ws_stream.split();

    // Enviar mensagem de identificação
    let id_message = serde_json::json!({
        "type": "device",
        "device": "RustClient"
    });
    write
        .send(Message::Text(id_message.to_string()))
        .await
        .expect("Falha ao enviar mensagem de identificação");
    info!("Mensagem de identificação enviada: {:?}", id_message);

    // Clone para mover para a tarefa
    let devices_clone = devices.clone();

    // Tarefa para lidar com mensagens recebidas
    tokio::spawn(async move {
        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    info!("Mensagem recebida: {}", text);

                    // Parse da mensagem JSON
                    match serde_json::from_str::<IncomingMessage>(&text) {
                        Ok(msg) => {
                            info!("Mensagem JSON parseada: {:?}", msg);
                            if msg.msg_type == "message" {
                                if let Some(content) = msg.content {
                                    info!("Comando recebido: {}", content);
                                    handle_message(&content, devices_clone.clone()).await;
                                } else {
                                    info!("Campo 'content' ausente na mensagem.");
                                }
                            } else {
                                info!("Tipo de mensagem desconhecido: {}", msg.msg_type);
                            }
                        }
                        Err(e) => {
                            eprintln!("Erro ao parsear mensagem JSON: {}", e);
                        }
                    }
                }
                Ok(_) => {
                    info!("Mensagem WebSocket ignorada.");
                }
                Err(e) => {
                    eprintln!("Erro no WebSocket: {}", e);
                    break;
                }
            }
        }
    });

    // Manter a função principal rodando
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}

async fn handle_message(content: &str, devices: Arc<Mutex<Vec<Device>>>) {
    info!("Processando mensagem: {}", content);

    match content.to_lowercase().as_str() {
        "on" => {
            info!("Comando: Ligar todos os dispositivos.");
            let mut devices = devices.lock().await;
            let futures = devices.iter_mut().map(|device| async {
                info!("Tentando ligar o dispositivo {}...", device.ip);
                match device.handler.on().await {
                    Ok(_) => info!("Dispositivo {} ligado com sucesso.", device.ip),
                    Err(e) => {
                        eprintln!("Falha ao ligar dispositivo {}: {}", device.ip, e);
                        info!("Detalhes do erro ao ligar dispositivo {}: {:?}", device.ip, e);
                    },
                }
            });
            futures::future::join_all(futures).await;
        }
        "off" => {
            info!("Comando: Desligar todos os dispositivos.");
            let mut devices = devices.lock().await;
            let futures = devices.iter_mut().map(|device| async {
                info!("Tentando desligar o dispositivo {}...", device.ip);
                match device.handler.off().await {
                    Ok(_) => info!("Dispositivo {} desligado com sucesso.", device.ip),
                    Err(e) => {
                        eprintln!("Falha ao desligar dispositivo {}: {}", device.ip, e);
                        info!("Detalhes do erro ao desligar dispositivo {}: {:?}", device.ip, e);
                    },
                }
            });
            futures::future::join_all(futures).await;
        }
        "green" => {
    info!("Comando: Definir cor para verde com 100% de brilho em todos os dispositivos.");
    let mut devices = devices.lock().await;
    let futures = devices.iter_mut().map(|device| async {
        info!("Tentando definir cor verde para o dispositivo {} com 100% de brilho...", device.ip);
        match device.handler.set().brightness(100).hue_saturation(120, 100).send(&device.handler).await {
            Ok(_) => info!("Dispositivo {} definido para verde com 100% de brilho com sucesso.", device.ip),
            Err(e) => {
                eprintln!("Falha ao definir cor verde para dispositivo {}: {}", device.ip, e);
                info!("Detalhes do erro ao definir cor verde para dispositivo {}: {:?}", device.ip, e);
            },
        }
    });
    futures::future::join_all(futures).await;
}
"red" => {
    info!("Comando: Definir cor para vermelho com 100% de brilho em todos os dispositivos.");
    let mut devices = devices.lock().await;
    let futures = devices.iter_mut().map(|device| async {
        info!("Tentando definir cor vermelho para o dispositivo {} com 100% de brilho...", device.ip);
        match device.handler.set().brightness(100).hue_saturation(0, 100).send(&device.handler).await {
            Ok(_) => info!("Dispositivo {} definido para vermelho com 100% de brilho com sucesso.", device.ip),
            Err(e) => {
                eprintln!("Falha ao definir cor vermelho para dispositivo {}: {}", device.ip, e);
                info!("Detalhes do erro ao definir cor vermelho para dispositivo {}: {:?}", device.ip, e);
            },
        }
    });
    futures::future::join_all(futures).await;
}
        "ll" => {
    info!("Comando: Definir luz para branco quente com baixa luminosidade (5%) em todos os dispositivos.");
    let mut devices = devices.lock().await;
    let futures = devices.iter_mut().map(|device| async {
        info!("Tentando definir luz branca quente com baixa luminosidade para o dispositivo {}...", device.ip);
        match device.handler.set().brightness(5).color_temperature(2700).send(&device.handler).await {
            Ok(_) => info!("Dispositivo {} definido para branco quente com 5% de luminosidade com sucesso.", device.ip),
            Err(e) => {
                eprintln!("Falha ao definir luz branca quente com baixa luminosidade para dispositivo {}: {}", device.ip, e);
                info!("Detalhes do erro ao definir luz branca quente com baixa luminosidade para dispositivo {}: {:?}", device.ip, e);
            },
        }
    });
    futures::future::join_all(futures).await;
}

        "white" => {
    info!("Comando: Definir luz para branco quente em todos os dispositivos.");
    let mut devices = devices.lock().await;
    let futures = devices.iter_mut().map(|device| async {
        info!("Tentando definir luz branca quente para o dispositivo {}...", device.ip);
        match device.handler.set().brightness(100).color_temperature(2700).send(&device.handler).await { // 2700K para luz branca quente
            Ok(_) => info!("Dispositivo {} definido para branco quente com sucesso.", device.ip),
            Err(e) => {
                eprintln!("Falha ao definir luz branca quente para dispositivo {}: {}", device.ip, e);
                info!("Detalhes do erro ao definir luz branca quente para dispositivo {}: {:?}", device.ip, e);
            },
        }
    });
    futures::future::join_all(futures).await;
}
    "cycle" => {
    info!("Comando: Ciclo contínuo de cores iniciado.");
    let devices = devices.clone();
    tokio::spawn(async move {
        cycle_colors_continuous(devices, 60).await; // Ciclo contínuo com duração de 60 segundos
    });
}



        _ => {
            info!("Comando desconhecido recebido: {}", content);
        }
    }
}

async fn cycle_colors_continuous(devices: Arc<Mutex<Vec<Device>>>, duration: u64) {
    let steps = 360; // Total de variações de Hue para cobrir todo o espectro de cores (0 a 360)
    let step_duration = duration as f64 / steps as f64; // Tempo entre cada passo da transição

    info!("Iniciando ciclo de cores contínuo com duração de {} segundos.", duration);

    for step in 0..steps {
        let hue = step % 360; // Garante que o Hue esteja dentro do intervalo válido

        {
            let mut devices = devices.lock().await;
            let futures = devices.iter_mut().map(|device| async {
                info!("Alterando cor do dispositivo {} para Hue {}...", device.ip, hue);
                match device.handler.set().brightness(100).hue_saturation(hue, 100).send(&device.handler).await {
                    Ok(_) => info!("Dispositivo {} alterado para Hue {} com sucesso.", device.ip, hue),
                    Err(e) => {
                        eprintln!("Falha ao alterar cor para Hue {} no dispositivo {}: {}", hue, device.ip, e);
                        info!("Detalhes do erro ao alterar cor no dispositivo {}: {:?}", device.ip, e);
                    },
                }
            });
            futures::future::join_all(futures).await;
        }

        // Aguardar o tempo de transição para o próximo passo
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(step_duration)).await;
    }

    info!("Ciclo contínuo de cores completo.");
}

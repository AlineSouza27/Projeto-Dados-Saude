"""Gera PDF com documentação do modelo relacional de dengue."""

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from datetime import datetime

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

pdf_path = "database/Modelo_Relacional_Dengue.pdf"
doc = SimpleDocTemplate(pdf_path, pagesize=landscape(A4), topMargin=0.5*cm, bottomMargin=0.5*cm)
story = []
styles = getSampleStyleSheet()

# Estilos customizados
title_style = ParagraphStyle(
    'CustomTitle',
    parent=styles['Heading1'],
    fontSize=24,
    textColor=colors.HexColor('#1f4788'),
    spaceAfter=30,
    alignment=TA_CENTER,
    fontName='Helvetica-Bold'
)

heading_style = ParagraphStyle(
    'CustomHeading',
    parent=styles['Heading2'],
    fontSize=14,
    textColor=colors.HexColor('#2e5c8a'),
    spaceAfter=12,
    spaceBefore=12,
    fontName='Helvetica-Bold'
)

# ============================================================================
# DEFINIÇÃO DAS TABELAS
# ============================================================================

TABELAS = {
    'PACIENTES': {
        'descricao': 'Dados demográficos e pessoais do paciente',
        'campos': [
            ('id_paciente', 'SERIAL', 'PK', 'Identificador único do paciente'),
            ('idade', 'INT', '', 'Idade em anos'),
            ('sexo', 'CHAR(1)', '', 'Sexo (M/F)'),
            ('raca', 'VARCHAR(50)', '', 'Categoria racial'),
            ('escolaridade', 'VARCHAR(10)', '', 'Nível educacional'),
            ('ocupacao', 'INT', 'FK opt', 'Código da ocupação'),
            ('gestante', 'INT', '', 'Código de gestação (1=sim, 5=não)'),
            ('ano_nascimento', 'INT', '', 'Ano de nascimento'),
        ]
    },
    'LOCALIZACOES': {
        'descricao': 'Dados geográficos de notificação e residência',
        'campos': [
            ('id_localizacao', 'SERIAL', 'PK', 'Identificador único'),
            ('uf_notificacao', 'VARCHAR(2)', '', 'UF onde foi notificado'),
            ('municipio_notif', 'INT', '', 'Código município notificação'),
            ('regiao_notif', 'INT', '', 'Região notificação'),
            ('unidade_saude', 'INT', '', 'ID unidade de saúde'),
            ('uf_residencia', 'VARCHAR(2)', '', 'UF de residência'),
            ('municipio_resi', 'INT', '', 'Código município residência'),
            ('regiao_resi', 'INT', '', 'Região residência'),
            ('pais', 'INT', '', 'País (1=Brasil)'),
        ]
    },
    'DIAGNOSTICOS': {
        'descricao': 'Resultados de testes laboratoriais',
        'campos': [
            ('id_diagnostico', 'SERIAL', 'PK', 'Identificador único'),
            ('data_soro_1', 'DATE', '', 'Data coleta sorologia 1'),
            ('resultado_soro_1', 'INT', '', 'Resultado sorologia 1'),
            ('data_pcr', 'DATE', '', 'Data coleta PCR'),
            ('resultado_pcr', 'INT', '', 'Resultado PCR'),
            ('data_ns1', 'DATE', '', 'Data coleta NS1'),
            ('resultado_ns1', 'INT', '', 'Resultado NS1'),
            ('data_viral', 'DATE', '', 'Data isolamento viral'),
            ('resultado_viral', 'INT', '', 'Resultado isolamento'),
            ('sorotipo', 'INT', '', 'Sorotipo identificado (1-4)'),
        ]
    },
    'CASES': {
        'descricao': 'Tabela central com informações do caso de dengue',
        'campos': [
            ('id_case', 'SERIAL', 'PK', 'Identificador único do caso'),
            ('id_paciente', 'INT', 'FK', 'Referência para pacientes'),
            ('id_localizacao', 'INT', 'FK', 'Referência para localizacoes'),
            ('id_diagnostico', 'INT', 'FK opt', 'Referência para diagnósticos'),
            ('dt_notificacao', 'DATE', '', 'Data da notificação'),
            ('dt_sin_pri', 'DATE', '', 'Data do primeiro sintoma'),
            ('dt_encerra', 'DATE', '', 'Data de encerramento'),
            ('sem_notif', 'INT', '', 'Semana epidemiológica'),
            ('ano', 'INT', '', 'Ano da notificação'),
            ('tp_notif', 'INT', '', 'Tipo de notificação'),
            ('criterio', 'INT', '', 'Critério diagnóstico'),
            ('classificacao_final', 'INT', '', 'Classificação final'),
            ('evolucao', 'INT', '', 'Evolução (1=cura, 2=óbito)'),
            ('dt_obito', 'DATE', '', 'Data do óbito (se aplicável)'),
            ('hospitalizado', 'INT', '', 'Hospitalizado (1=sim, 2=não)'),
            ('data_internacao', 'DATE', '', 'Data de internação'),
        ]
    },
    'CASOS_SINTOMAS': {
        'descricao': 'Vincula casos aos sintomas apresentados (Tabela Associativa)',
        'campos': [
            ('id_caso_sintoma', 'SERIAL', 'PK', 'Identificador único'),
            ('id_case', 'INT', 'FK', 'Referência para cases'),
            ('id_sintoma', 'INT', 'FK', 'Referência para sintomas'),
            ('presente', 'INT', '', 'Presença do sintoma (1=sim, 0=não)'),
            ('data_inicio', 'DATE', '', 'Data de início do sintoma'),
        ]
    },
    'CASOS_FATORES_RISCO': {
        'descricao': 'Vincula casos aos fatores de risco identificados',
        'campos': [
            ('id_caso_fator', 'SERIAL', 'PK', 'Identificador único'),
            ('id_case', 'INT', 'FK', 'Referência para cases'),
            ('id_fator', 'INT', 'FK', 'Referência para fatores_risco'),
            ('presente', 'INT', '', 'Presença do fator (1=sim, 0=não)'),
            ('observacao', 'TEXT', '', 'Observações adicionais'),
        ]
    },
    'MANIFESTACOES_HEMOR': {
        'descricao': 'Manifestações hemorrágicas identificadas por caso',
        'campos': [
            ('id_manifestacao', 'SERIAL', 'PK', 'Identificador único'),
            ('id_case', 'INT', 'FK', 'Referência para cases'),
            ('petequias', 'INT', '', 'Petéquias presentes'),
            ('epistaxe', 'INT', '', 'Epistaxe (sangramento nasal)'),
            ('gengivorragia', 'INT', '', 'Sangramento gengival'),
            ('metrorragia', 'INT', '', 'Metrorragia'),
            ('hematuria', 'INT', '', 'Hematúria'),
            ('sangramento_gi', 'INT', '', 'Sangramento GI'),
            ('data_manifestacao', 'DATE', '', 'Data da manifestação'),
        ]
    },
    'ALERTAS_GRAVES': {
        'descricao': 'Sinais de alerta e gravidade em dengue grave',
        'campos': [
            ('id_alerta', 'SERIAL', 'PK', 'Identificador único'),
            ('id_case', 'INT', 'FK', 'Referência para cases'),
            ('hipotensao', 'INT', '', 'Hipotensão presente'),
            ('plaquetas_criticas', 'INT', '', 'Plaquetas críticas'),
            ('vomitos_persistentes', 'INT', '', 'Vômitos persistentes'),
            ('sangramento', 'INT', '', 'Sangramento'),
            ('hematocrito_alt', 'INT', '', 'Hematócrito alterado'),
            ('dor_abdominal', 'INT', '', 'Dor abdominal'),
            ('letargia', 'INT', '', 'Letargia'),
            ('hepatomegalia', 'INT', '', 'Hepatomegalia'),
            ('data_alerta', 'DATE', '', 'Data do alerta'),
        ]
    },
    'SINTOMAS': {
        'descricao': 'Tabela dimensão com lista catalogada de sintomas',
        'campos': [
            ('id_sintoma', 'SERIAL', 'PK', 'Identificador único'),
            ('nome', 'VARCHAR(100)', 'UK', 'Nome do sintoma (único)'),
            ('descricao', 'TEXT', '', 'Descrição detalhada'),
            ('categoria', 'VARCHAR(50)', '', 'Categoria (Dor, GI, etc)'),
        ]
    },
    'FATORES_RISCO': {
        'descricao': 'Tabela dimensão com lista de fatores de risco',
        'campos': [
            ('id_fator', 'SERIAL', 'PK', 'Identificador único'),
            ('nome', 'VARCHAR(100)', 'UK', 'Nome do fator (único)'),
            ('descricao', 'TEXT', '', 'Descrição detalhada'),
            ('categoria', 'VARCHAR(50)', '', 'Categoria (Doença crônica, etc)'),
        ]
    },
}

# ============================================================================
# CONSTRUIR DOCUMENTO
# ============================================================================

# Título
story.append(Paragraph("MODELO RELACIONAL DE DADOS", title_style))
story.append(Paragraph("Dengue - DATASUS", styles['Heading2']))
story.append(Paragraph(f"Gerado em {datetime.now().strftime('%d/%m/%Y às %H:%M')}", styles['Normal']))
story.append(Spacer(1, 0.5*cm))

# Visão geral
story.append(Paragraph("📊 Visão Geral", heading_style))
overview_text = """
<b>10 tabelas normalizadas</b> organizadas em 3 camadas:<br/>
<b>Fatos:</b> CASES (central) — <b>Dimensões:</b> PACIENTES, LOCALIZACOES, DIAGNOSTICOS, SINTOMAS, FATORES_RISCO —
<b>Associativas:</b> CASOS_SINTOMAS, CASOS_FATORES_RISCO, MANIFESTACOES_HEMOR, ALERTAS_GRAVES
"""
story.append(Paragraph(overview_text, styles['Normal']))
story.append(Spacer(1, 0.5*cm))

# Documentação de cada tabela
for tbl_name, tbl_info in TABELAS.items():
    story.append(Paragraph(f"🔹 {tbl_name}", heading_style))
    story.append(Paragraph(f"<i>{tbl_info['descricao']}</i>", styles['Normal']))
    
    # Tabela de campos
    campos_data = [['Campo', 'Tipo', 'Constraint', 'Descrição']]
    for campo, tipo, const, desc in tbl_info['campos']:
        campos_data.append([
            Paragraph(f"<b>{campo}</b>", styles['Normal']),
            Paragraph(f"<font size=9>{tipo}</font>", styles['Normal']),
            Paragraph(f"<font size=9>{const}</font>", styles['Normal']),
            Paragraph(f"<font size=8>{desc}</font>", styles['Normal']),
        ])
    
    tbl = Table(campos_data, colWidths=[2.2*cm, 2.2*cm, 1.8*cm, 6*cm])
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e5c8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')]),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
    ]))
    
    story.append(tbl)
    story.append(Spacer(1, 0.3*cm))

# Página com diagrama descritivo
story.append(PageBreak())
story.append(Paragraph("📐 Diagrama de Relacionamentos", heading_style))

diagram_text = """
<pre>
┌─────────────────────────────────────────────────────────────┐
│                          CASES                              │
│                      (Tabela Central)                       │
│  ┌─────────────────┬──────────────────────────────────────┐ │
│  │• id_case (PK)   │                                      │ │
│  │• dt_notificacao │ ─────────────────────────────────┐  │ │
│  │• evolucao       │                                  │  │ │
│  │• hospitalizado  │                                  │  │ │
│  │• classificação  │                                  │  │ │
│  │                 │   ┌──────────────┐               │  │ │
│  │                 └──→│ PACIENTES    │               │  │ │
│  │       ┌────────────→│ LOCALIZACOES │←──────────┐   │  │ │
│  │       │             │ DIAGNOSTICOS │           │   │  │ │
│  │       │             └──────────────┘           │   │  │ │
│  └───────┼─────────────────────────────────────────┼───┘  │
│          │                                         │       │
│    ┌─────▼──────────────────────────────────────┬─▼──┐    │
│    │ CASOS_SINTOMAS          CASOS_FATORES_RISCO    │    │
│    │ (Associativa)           (Associativa)          │    │
│    │ ↓                        ↓                      │    │
│    │ SINTOMAS (Dimensão)   FATORES_RISCO (Dim)    │    │
│    └────────────────────────────────────────────────┘    │
│                                                         │
│    MANIFESTACOES_HEMOR          ALERTAS_GRAVES      │
│    ↓                            ↓                    │
│    Detalhes hemorrágicos   Sinais de alerta grave │
│                                                         │
└─────────────────────────────────────────────────────────────┘
</pre>
"""

story.append(Paragraph(diagram_text, styles['Normal']))
story.append(Spacer(1, 0.5*cm))

# Estatísticas
story.append(Paragraph("📈 Estatísticas do Schema", heading_style))
stats_text = f"""
<b>Total de Tabelas:</b> 10 (4 dimensões, 4 fatos/associativas, 2 dimensões)<br/>
<b>Total de Campos:</b> 130+ campos originais mapeados para ~100 campos normalizados<br/>
<b>Relacionamentos:</b> 1:N para tabelas associativas, N:N via associativas<br/>
<b>Chaves Primárias:</b> SERIAL (auto-incremento)<br/>
<b>Chaves Estrangeiras:</b> ON DELETE CASCADE para manter integridade<br/>
<b>Índices:</b> Criados para performance em queries frequentes<br/>
<b>Views:</b> 3 views pré-construídas para análise agregada
"""
story.append(Paragraph(stats_text, styles['Normal']))

# Build PDF
doc.build(story)
print(f"✓ PDF gerado com sucesso: {pdf_path}")

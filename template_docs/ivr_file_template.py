FILE_HEADER = '''
\\documentclass{report}
\\usepackage{longtable}
\\usepackage[margin=1.0cm]{geometry}

\\title{Laudo Quantitativo}
\\author{}
\\date{\\today}


\\begin{document}

\\maketitle
'''

DESCRICAO = '''    
\\section{{Descrição}}
\\subsection{{0) PROCESSO}}
\\begin{{itemize}}
	\\item{{Número do processo: {numero_processo}}} 
\\end{{itemize}}


\\subsection{{1) POLO ATIVO}}
\\begin{{itemize}}
	\\item{{Razão social: {razao_social}}} 
	\\item{{Nome fantasia: {nome_fantasia}}}
	\\item{{Código CNES: {cnes}}}
	\\item{{UF: {estado}}} 
	\\item{{Cidade: {cidade}}} 
\\end{{itemize}}


\\subsection{{2) POLO PASSIVO}}
\\begin{{itemize}}
	\\item{{União Federal}}
\\end{{itemize}}
'''

METODOLOGIA = '''
\\section{Metodologia}
Os dados utilizados na Quantificação e Qualificação dos procedimentos hospitalares/ambulatoriais do SUS foram extraídos diretamente das fontes oficiais disponibilizadas pelo próprio SUS, através de protocolo FTP nos endereços disponibilizados pelo SUS em suas plataformas, fazendo download e, posteriormente, importando em formato csv, com toda rastreabilidade e observância aos critérios de segurança da informação.
Nossa metodologia, no cumprimento de sentença, permite quaisquer validações de origem e rastreabilidade das informações extraídas e utilizadas, concedendo, com isso, segurança e rastreabilidade ao número aqui apresentado.
No cálculo de Atualização Monetária foram considerados os indicadores, conforme Resolução CJF N\\textsuperscript{o} 784/2022, de 08/08/22, publicada em 11/08/22, onde aprovou a alteração do Manual de Orientação de Procedimentos para os Cálculos na Justiça Federal (anexo à Resolução CJF N\\textsuperscript{o} 784/22), cuja orientação constante no Capítulo 4 (Liquidação de Sentença) é que, sendo devedora a Fazenda Pública em ações não tributárias, quanto às prestações devidas até dez/2021: a) o crédito será consolidado tendo por base o mês de dez./2021 pelos critérios de juros e correção monetária até então aplicáveis (definidos na Sentença); e b) sobre o valor consolidado do crédito em dez/2021 (principal corrigido + juros moratórios) incidirá a taxa Selic e partir de jan/2022) (\\S 1\\textsuperscript{o} do art. 22 da Resolução CNJ N\\textsuperscript{o} 303/2019, com redação dada pelo art. 6\\textsuperscript{o} da Resolução CNJ  N\\textsuperscript{o} 448/2022).
'''

CONCLUSAO = '''
\\section{{Conclusões}}
Com base nas informações extraídas do DATASUS de procedimentos hospitalares e ambulatoriais (valores e quantidades), onde este perito processou 100\\% (cem por cento) destas informações e, por último, aplicando as correções monetárias e juros de mora, tem-se o total da ação de cumprimento de sentença de R\\$ \\textbf{{{valor_total}}}.
'''

TOTAL_HEADER = '''
\\section{Resumo Total}
\\begin{longtable}[c]{|p{1.7cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|}
	\\caption{Resumo total} \\\\ \\hline
	\\textbf{Diferença devida e não paga} &
	\\textbf{Atualização} &
	\\textbf{Valor total devido}
	\\endhead \\hline
'''

TOTAL_FOOTER = '''
    \\end{longtable}
'''

YEAR_HEADER = '''
\\section{Resumo Anual}
\\begin{longtable}[c]{|p{1.7cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|}
	\\caption{Resumo ano a ano} \\\\ \\hline
	\\textbf{Ano} &
	\\textbf{Diferença devida e não paga} &
	\\textbf{Atualização} &
	\\textbf{Valor total devido}
	\\endhead \\hline
'''

YEAR_FOOTER = '''
    \\end{longtable}
'''

MONTH_HEADER = '''
\\section{Resumo mensal}
\\begin{longtable}[c]{|p{1.7cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|p{2.3cm}|p{2.3cm}|c|}
	\\caption{Resumo mês a mês} \\\\ \\hline
	\\textbf{Mês} &
	\\textbf{Valor original pago pelo SUS} &
	\\textbf{Diferença devida e não paga} &
	\\textbf{Atualização antes de 01/2022} &
	\\textbf{Selic a partir de 01/2022} &
	\\textbf{Valor total devido}
	\\endhead \\hline
'''

MONTH_FOOTER = '''
\\end{longtable}
'''

FILE_FOOTER =  '''\\end{document}'''

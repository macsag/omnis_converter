import re


# 4. creators names normalization
def prepare_name_for_indexing(descriptor_name: str) -> str:
    if descriptor_name:
        # 4.1 wszystko, co nie jest literą lub cyfrą zastępowane jest spacją
        descriptor_name = ''.join(char.replace(char, ' ') if not char.isalnum() else char for char in descriptor_name)

        # 4.2 wielokrotne białe znaki są redukowane do jednej spacji
        match = re.finditer(r'\s{2,}', descriptor_name)
        for m_object in match:
            descriptor_name = descriptor_name.replace(m_object.group(0), ' ')

        # 4.3 białe znaki z początku i końca są usuwane
        descriptor_name = descriptor_name.strip()

        # 4.4 wszystkie znaki podniesione do wielkich liter
        descriptor_name = descriptor_name.upper()

    return descriptor_name


# titles normalization
def normalize_title(title: str) -> str:
    if title:
        # 1 należy usunąć z początku i końca tytułu wszystkie białe znaki
        title = title.strip()

        # 2 należy usunąć z końca tytułu 0 lub więcej białych znaków i dowolny znak z listy [/:;,=.]
        match = re.search(r'\s*[/:;,=.]$', title)
        if match:
            title = title[:match.span(0)[0]]

        # 3 należy usunąć ciąg znaków przypominający datę na końcu tytułu
        # TODO

        return title

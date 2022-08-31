import re


# CUSTOM PARSERS FOR FIGHTER CARDS
def extract_info(tags):
    img = parse_tags(tags, 'img', 'image-style-teaser')
    nick_name = parse_tags(tags, 'div', 'field field--name-nickname field--type-string field--label-hidden')
    name = parse_tags(tags, 'span', 'c-listing-athlete__name')
    weight_class = parse_tags(tags, 'div', 'field field--name-stats-weight-class field--type-entity-reference field--label-hidden field__items')
    record = parse_tags(tags, 'span', 'c-listing-athlete__record')

    return {'img': img, 'nick-name': clean(nick_name), 'name': clean(name), 'weight-class': clean(weight_class), 'record': record}

def parse_tags(ex, tag, attr_name):
    try:
        if tag == 'img':
            text = ex.find(tag, attrs={'class': attr_name})['src']
        else:
            text = ex.find(tag, attrs={'class': attr_name}).text
    except:
        text = ''
    return text

def clean(arg):
    return arg.strip().replace('"', '').replace("'", '')



######################################################################################



# CUSTOM PARSERS FOR STATS DATA
def parse_soup(soup, fighter, slug):
    record_tags = soup.find_all('div', {'class':'c-record__promoted'})
    accuracy_tags = soup.find_all('dl', {'class':'c-overlap__stats'})
    stats_tags = soup.find_all('div', {'class': re.compile('c-stat-compare__group')})
    wins_tags = soup.find_all('div', {'class': 'c-stat-3bar__group'})
    meta_tags = soup.find_all('div', {'class': 'c-bio__field'})

    records = extract(record_tags, 'c-record__promoted-text', 'c-record__promoted-figure')
    accuracy = extract(accuracy_tags, 'c-overlap__stats-text', 'c-overlap__stats-value', 'accuracy')
    stats = extract(stats_tags, 'c-stat-compare__label', 'c-stat-compare__number', 'stats')
    wins = extract(wins_tags, 'c-stat-3bar__label', 'c-stat-3bar__value', 'wins')
    meta = extract(meta_tags, 'c-bio__label', 'c-bio__text')

    return_obj = {'slug':slug}

    for obj in [records, accuracy, stats, wins, meta]:
        if obj is not None:
            return_obj.update(obj)

    return return_obj

def extract(tags, label, value, spec=None):
    if tags is []: return
    
    obj = {}
    for tag in tags:
        labels = tag.find_all('div', {'class':label}) if spec != 'accuracy' else tag.find_all('dt', {'class':label})
        values = tag.find_all('div', {'class':value}) if spec != 'accuracy' else tag.find_all('dd', {'class':value})
        
        if labels == []: return
        
        for idx in range(len(labels)):
            try:
                key, val = labels[idx].text.strip(), values[idx].text.strip()

                if '%' in val and spec == 'stats': val = float(val[0:2])/100
                if spec == 'wins': val = val.split(' ')[0]
                if key == '': continue

                obj[key] = val
            except:
                pass

    return obj


if __name__ == "__main__":
    pass
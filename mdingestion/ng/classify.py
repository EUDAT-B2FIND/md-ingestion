import os
import re
import json
from collections import OrderedDict
import Levenshtein as lvs

import logging

CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'mapfiles')


class Classify(object):
    def __init__(self):
        self.disctab = self.load_list()

    def load_list(self):
        fname = os.path.join(CFG_DIR, 'b2find_disciplines.json')
        with open(fname) as fp:
            disctab = json.load(fp)['disciplines']
        return disctab

    def map_discipline(self, invalue):
        """
        Convert disciplines along B2FIND disciplinary list
        """
        retval = []
        if isinstance(invalue, list):
            # seplist = [re.split(r"[;&\xe2]", i) for i in invalue]
            swlist = [re.findall(r"[\w']+", i) for i in invalue]
            inlist = swlist  # +seplist
            inlist = [item for sublist in inlist for item in sublist]  # ???
        else:
            inlist = re.split(r'[;&\s]\s*', invalue)
            inlist.append(invalue)
        for indisc in inlist:
            logging.debug(f'Next input discipline value {indisc} of type {type(indisc)}')
            indisc = indisc.replace('\n', ' ').replace('\r', ' ').strip().title()
            maxr = 0.0
            maxdisc = ''
            for line in self.disctab:
                line = re.split(r'#', line)
                try:
                    if len(line) < 3:
                        logging.critical(f'Missing base element in dicipline array {line}')
                        raise Exception('Missing base element in dicipline array')
                    else:
                        disc = line[2].strip()
                        r = lvs.ratio(indisc, disc)
                except Exception as e:
                    logging.error(f'{e} : {indisc} of type {type(indisc)} can not compared to {disc} of type {type(disc)}')  # noqa
                    continue
                if r > maxr:
                    maxdisc = disc
                    maxr = r
                    logging.debug(f'--- {line} |{indisc}|{disc}|{r}|{maxr}')
                    rethier = line
            if maxr == 1 and indisc == maxdisc:
                logging.info(f'  | Perfect match of >{indisc}< : nothing to do, DiscHier {line}')
                retval.append(indisc.strip())
            elif maxr > 0.90:
                logging.info(f'   | Similarity ratio {maxr} is > 0.90 : replace value >>{indisc}<< with best match --> {maxdisc}')  # noqa
                # return maxdisc
                retval.append(indisc.strip())
            else:
                logging.debug(f'   | Similarity ratio {maxr} is < 0.90 compare value >>{indisc}<< and discipline >>{maxdisc}<<')  # noqa
                continue
        if len(retval) > 0:
            retval = list(OrderedDict.fromkeys(retval))  # this elemenates real duplicates
            return (';'.join(retval), rethier)
        else:
            return ('Various', [])

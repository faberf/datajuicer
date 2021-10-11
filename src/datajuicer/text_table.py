import itertools

def get_shape(s):
    lines = s.split("\n")
    width = max([len(line) for line in lines])
    return len(lines), width


def pad(s, h=None, w=None, align="rl"):
    shape = get_shape(s)
    if not h is None:
        shape[0] = h
    if not w is None:
        shape[1] = w
        
    lines = s.split("\n")
    for i, line in enumerate(lines):
        padding = " " * (shape[1] - len(line))

        if "r" in align:
            lines[i] = padding + line
        elif "c" in align:
            pad_left = pad_right = " " * (len(padding)/2)
            if len(padding)%2 == 1:
                pad_right += " "
            lines[i] = pad_left + line + pad_right
        else:
            lines[i] = line + padding

    vert_pad = [" " * shape[1]] * (shape[0] - len(lines))

    if "b" in align:
        return "\n".join(vert_pad + lines)
    if "m" in align:
        pad_above = [" " * shape[1]] * (len(vert_pad)/2)
        pad_below = [" " * shape[1]] * (len(vert_pad)/2)
        if len(vert_pad)%2 == 1:
            pad_below.append(" " * shape[1])
        return "\n".join(pad_above + lines + pad_below)
    return "\n".join(lines + vert_pad)

def insert_a_into_b(a, b, r, c):
    a_lines = a.split('\n')
    b_lines = b.split('\n')
    for a_row, b_row in enumerate(range(r,r+len(a_lines))):
        b_lines[b_row] = b_lines[b_row][0:c] + a_lines[a_row] + b_lines[b_row][c + len(a_lines[a_row]):]
    return "\n".join(b_lines)

def max_shape(s_list):
    if type(s_list) is str:
        return get_shape(s_list)
    shapes = [max_shape(s) for s in s_list]
    max_height = max([sh[0] for sh in shapes])
    max_width = max([sh[1] for sh in shapes])
    return max_height, max_width

def horizontal_align(s_list, col_aligns):
    h, _ = max_shape(s_list)
    out = [""] * h
    for s in s_list:
        _,w = get_shape(s)
        s = pad(s,h,w)
        for i, line in enumerate(s):
            out[i] += line
    return "\n".join(out)

def _vertical_align(s_list, row_aligns):

    return "\n".join([s for s in s_list])

def _grid_align(s_list_list, row_aligns, col_aligns):
    transposed = list(map(list, zip(*s_list_list)))
    row_heigths = []
    for row in s_list_list:
        row_heigths.append(max_shape(row)[0])

    cols = []
    for col in transposed:
        for i,s in enumerate(col):
            col[i] = pad(s,h=row_heigths[i])

        cols.append(vertical_align(col))
    return horizontal_align(cols)



TL_CORNER = "+"
TR_CORNER = "+"
BL_CORNER = "+"
BR_CORNER = "+"
CROSS = "+"

B_BAR = "-"
T_BAR = "-"
R_BAR = "|"
L_BAR = "|"

def full_border(s):
    h, w = get_shape(s)
    t_border = T_BAR * w
    b_border = B_BAR * w
    l_border = "\n".join([L_BAR]*h) 
    r_border = "\n".join([R_BAR]*h) 
    return grid_align(
        [
            [   TL_CORNER,  t_border,   TR_CORNER   ],
            [   l_border,   s,          r_border    ],
            [   BL_CORNER,  b_border,   BR_CORNER   ]
        ]
    )


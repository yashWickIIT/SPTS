tailwind.config = {
  theme: {
    extend: {
      colors: {
        sage: {
          DEFAULT: '#9FC2AD',
          light: '#BDD1C6',
          dark: '#7CA68E',
        },
        charcoal: '#374151',
        slate: '#6C7280',
        muted: '#A3A7AF',
        error: '#F87171',
      },
      fontFamily: {
        outfit: ['Outfit', 'sans-serif'],
        dancing: ['Dancing Script', 'cursive'],
      },
      // borderRadius: {
      //   'xl': '16px',
      //   '2xl': '24px',
      // },
      boxShadow: {
        'premium': '0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04)',
        'glass': '0 10px 15px -3px rgba(0,0,0,0.1), 0 4px 6px -2px rgba(0,0,0,0.05)',
      },
      flex: {
        '1.2': '1.2 1.2 0%',
      },
      animation: {
        'float': 'float 6s ease-in-out infinite',
      },
      keyframes: {
        float: {
          '0%, 100%': { transform: 'translateY(0)' },
          '50%': { transform: 'translateY(-15px)' },
        }
      }
    }
  }
}
